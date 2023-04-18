import streamlit as st
import pandas as pd
import time
import polars as pl
import duckdb


#additions
#strikes landed per minute over time with % over time
#strikes absored per fight

###################################
def _max_width_():
    max_width_str = f"max-width: 1800px;"
    st.markdown(
        f"""
    <style>
    .reportview-container .main .block-container{{
        {max_width_str}
    }}
    </style>    
    """,
        unsafe_allow_html=True,
    )


st.set_page_config(page_icon="👊", page_title="UFC Stats Explorer v0.5", layout="wide")

########start of app
st.header('UFC Fight Stats explorer')

view = st.sidebar.radio('Select a view',('Fighter One Sheet','All Time Stats','Show all dataset samples'))


###################### data pull and clean

ed = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_event_details.csv")
ed_c = duckdb.sql("SELECT TRIM(EVENT) as EVENT, strptime(DATE, '%B %d, %Y') as  DATE, URL, LOCATION FROM ed")
fd = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_details.csv")
fed = duckdb.sql("SELECT TRIM(fd.EVENT) as EVENT, TRIM(fd.BOUT) as BOUT, fd.URL, DATE,LOCATION from ed_c inner join fd on ed_c.EVENT=fd.EVENT ")
fr = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_results.csv")
fr_cleaned = duckdb.sql("""SELECT trim(fr.EVENT) as EVENT, 
                             replace(trim(fr.BOUT),'  ',' ') as BOUT, 
                            trim(split_part(fr.BOUT, ' vs. ' ,1)) as FIGHTER1,
                            trim(split_part(fr.BOUT, ' vs. ', 2)) as FIGHTER2,
                            split_part(OUTCOME, '/' ,1) as FIGHTER1_OUTCOME,
                            split_part(OUTCOME, '/', 2) as FIGHTER2_OUTCOME,
                            WEIGHTCLASS,METHOD,ROUND,TIME,left("TIME FORMAT",1) as TIME_FORMAT,REFEREE,DETAILS,fr.URL,date 
                        from fr
                        left join fed on fed.URL = fr.URL""")
fs = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_stats.csv")
fs_cleaned = duckdb.sql("""SELECT EVENT,replace(trim(BOUT),'  ',' ') as BOUT,ROUND,trim(FIGHTER) as FIGHTER,KD,
                              split_part("SIG.STR.",' of ',1) sig_str_l,
                              split_part("SIG.STR.",' of ',2) sig_str_a,
                              split_part("TOTAL STR.",' of ',1) total_str_l,
                              split_part("TOTAL STR.",' of ',2) total_str_a,
                              split_part(TD,' of ',1) td_l,
                              split_part(TD,' of ',2) td_a,
                              "SUB.ATT","REV.",CTRL,
                              split_part(HEAD,' of ',1) head_str_l,
                              split_part(HEAD,' of ',2) head_str_a  
                              from fs WHERE FIGHTER IS NOT NULL """)
frd = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_details.csv")
ft = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_tott.csv")
fighters= duckdb.sql("SELECT trim(FIGHTER) as FIGHTER,HEIGHT,WEIGHT,REACH,STANCE,DOB,FIRST,LAST,NICKNAME,frd.URL from ft inner join frd on frd.URL = ft.URL")
########################
                      


#
if view =='Fighter One Sheet':
    st.text('Display all relevant fighter stats in just 1 click. Choose your fighter below to get started')
    fighter_list = duckdb.sql("SELECT FIGHTER from fighters  where DOB is not null group by 1 order by 1").df()
    f1, f2 = st.columns(2)
    with f1:
        fighter_filter = st.selectbox('Pick a fighter',options=fighter_list)
    with f2:
        st.radio("Analysis Length (under development)",('Career','Last 3 fights'),disabled=True)
    st.divider()
    fights = duckdb.sql("SELECT BOUT from fr_cleaned where FIGHTER1 = '{}' or FIGHTER2='{}'".format(fighter_filter,fighter_filter))

    winloss = duckdb.sql("SELECT case when FIGHTER1 = '{}' then FIGHTER1_OUTCOME else FIGHTER2_OUTCOME end result from fr_cleaned where FIGHTER1 = '{}' or FIGHTER2='{}' ".format(fighter_filter,fighter_filter,fighter_filter))
    last_fight= duckdb.sql("SELECT left(max(date),10) max_date, left( current_date() - max(date),10) days_since from fr_cleaned where FIGHTER1= '{}' or FIGHTER2='{}' ".format(fighter_filter,fighter_filter)).df()
    fighter_stats = duckdb.sql("SELECT * from fs_cleaned where BOUT in (select BOUT from fights) and FIGHTER ='{}' ".format(fighter_filter))
    cleaned_fighter_stats = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_str, sum(head_str_l::INTEGER) as head_str, sum(td_l::INTEGER) as td_l, round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2)  as td_rate, sum(kd::INTEGER) as kd, from fighter_stats").df()
    ko_wins = duckdb.sql("SELECT count(*) as s from fr_cleaned where ((FIGHTER1='{}' and FIGHTER1_OUTCOME='W') OR (FIGHTER2='{}' and FIGHTER2_OUTCOME='W')) and trim(METHOD)='KO/TKO' ".format(fighter_filter,fighter_filter)).df()
    
    opp_stats = duckdb.sql("SELECT * from fs_cleaned where BOUT in (select * from fights) and FIGHTER !='{}' ".format(fighter_filter))
    cleaned_opp_stats = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_abs ,sum(head_str_l::INTEGER) as head_abs,sum(td_l::INTEGER) as td_abs,round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2) as td_abs_rate,sum(kd::INTEGER) as kd_abs from opp_stats").df()
    ko_losses = duckdb.sql("SELECT count(*) as s from fr_cleaned where ((FIGHTER1='{}' and FIGHTER1_OUTCOME='L') OR (FIGHTER2='{}' and FIGHTER2_OUTCOME='L')) and trim(METHOD)='KO/TKO' ".format(fighter_filter,fighter_filter)).df()

    if fighter_filter:
        col1,col2,col3, col4 = st.columns(4)
        with col1:
            st.subheader('Highlights')
            w1,w2 = st.columns(2)
            with w1:
                st.metric('Total UFC Fights',value=len(fights.df()) )
            with w2:
                st.metric('Wins',value=len(duckdb.sql("SELECT * from winloss where result='W'").df()) )
                st.metric('Losses',value=len(duckdb.sql("SELECT * from winloss where result='L'").df()) )
            
            if len(fights.df()) >0:
                st.write('Latest fight - '+str(last_fight['max_date'].values[0])+' - '+str(last_fight['days_since'].values[0])+ ' ago')
            st.metric('KO/TKO Wins', value = int(ko_wins['s']) )
            st.metric('KO/TKO Losses', value = int(ko_losses['s']))
        with col2:
            st.subheader('Striking')
            st.metric('Significant Strikes Absored',value=int(cleaned_opp_stats['sig_abs']))
            st.metric('Head Strikes Absored',value=int(cleaned_opp_stats['head_abs']))
            st.metric('Significant Strikes Landed',value=int(cleaned_fighter_stats['sig_str']))
            st.metric('Head Strikes Landed',value=int(cleaned_fighter_stats['head_str']))
            st.metric('Knockdowns Landed',value=int(cleaned_fighter_stats['kd']))
            st.metric('Knockdowns Absored',value=int(cleaned_opp_stats['kd_abs']))
        with col3:
            st.subheader('Wrestling')
            st.metric('Total Takedowns Landed',value=int(cleaned_fighter_stats['td_l']),delta="{0:.0%}".format(round(float(cleaned_fighter_stats['td_rate']),2)))
            st.metric('Total Takedowns Given Up',value=int(cleaned_opp_stats['td_abs']),delta="{0:.0%}".format(round(float(cleaned_opp_stats['td_abs_rate']),2)))
        with col4:
            st.subheader('Advanced Stats')
            st.metric('Significant Strikes Differential',value=round(float(cleaned_fighter_stats['sig_str']/cleaned_opp_stats['sig_abs']),1))
            st.metric('Head Strikes Differential',value=round(float(cleaned_fighter_stats['head_str']/cleaned_opp_stats['head_abs']),1))
            st.metric('Power Differential (Knockdowns)',value=round(float(cleaned_fighter_stats['kd']/cleaned_opp_stats['kd_abs']),1))
            st.metric('Takedown Differential',value=round(float(cleaned_fighter_stats['td_l']/cleaned_opp_stats['td_abs']),1))
        st.divider()
        with st.expander("Career Results",expanded=False):
            st.write(duckdb.sql("SELECT * EXCLUDE (DATE,BOUT,WEIGHTCLASS,TIME_FORMAT,URL),left(DATE,10) date from fr_cleaned where FIGHTER1= '{}' or FIGHTER2='{}' order by date desc".format(fighter_filter,fighter_filter)).df())
    
    st.divider()
    with st.expander("Single Fight Stats",expanded=False):
        bout_filter = st.selectbox('Pick a bout',options=fights.df().drop_duplicates())

        if bout_filter:
            st.write(duckdb.sql("SELECT * EXCLUDE (BOUT,FIGHTER,EVENT) from fighter_stats where BOUT ='{}' ".format(bout_filter)).df().T)

elif view =='Show all dataset samples':
    st.write('Fighter Details (cleaned)')
    st.dataframe(duckdb.sql("SELECT * from fighters order by FIGHTER asc limit 5").df())
    st.write('Events & Fights (cleaned)')
    st.write(duckdb.sql("SELECT * from fed limit 5").df())
    st.write('Fight Results (cleaned)')
    st.write(duckdb.sql("SELECT * from fr_cleaned limit 5").df())
    st.write('Fight Stats')
    st.write(duckdb.sql("SELECT * from fs limit 5").df())    
else:
    c1, c2 = st.columns(2)
    with c1:
        st.write("Fights by month")
        st.area_chart(duckdb.sql("SELECT date_trunc('month',date) date,count(*) fights from fed group by 1 order by 1 asc").df().set_index("date"))
        st.write("Events by month")
        st.area_chart(duckdb.sql("SELECT date_trunc('month', date) date, count(distinct EVENT) events from fed group by 1 order by 1 asc").df().set_index("date"))
        
        
    with c2:
        st.write('Most experienced referees in the last 2 years')
        st.dataframe(duckdb.sql("SELECT REFEREE,count(*) fights from fr_cleaned where date between current_date() -730 and current_date() group by 1 order by 2 desc limit 10").df())
        st.write('Fighters fought in the last 2 years')
        st.write(str(duckdb.sql("""
                       SELECT count(distinct fighter) s from 
                        (SELECT FIGHTER1 fighter from fr_cleaned where date between current_date() -730 and current_date() group by 1 
                        UNION 
                        SELECT FIGHTER2 fighter from fr_cleaned where date between current_date() -730 and current_date() group by 1)
                        """).df()['s'].sum()))
        st.write('Most commonly used venues in the last 2 years')
        st.dataframe(duckdb.sql("SELECT LOCATION,count(distinct EVENT) events from fed where date between current_date() -730 and current_date() group by 1 order by 2 desc limit 10").df())
    
    st.write("1000 Most hit (head), knocked down and taken down fighters all time")        
    fighters = duckdb.sql("SELECT fighter FROM fs_cleaned GROUP BY 1 order by count(distinct BOUT) desc limit 1000").df()
    fighters['FIGHTER'] = fighters['FIGHTER'].str.replace("'", "") 
    fsc = fs_cleaned.df()
    fsc['FIGHTER'] = fsc['FIGHTER'].str.replace("'", "")
    fsc['BOUT'] = fsc['BOUT'].str.replace("'", "")
    str_results = pd.DataFrame()
    for f in fighters['FIGHTER']:
        query = f"SELECT '{f}' AS FIGHTER,count(distinct BOUT||EVENT) as FIGHTS,count(*) as ROUNDS, SUM(head_str_l::INTEGER) AS HEAD_STRIKES_ABSORED,sum(KD::INTEGER) as KD, sum(TD_L::INT) as TD FROM fs_cleaned WHERE BOUT LIKE '%{f}%' AND fighter != '{f}' "
        result = duckdb.sql(query).df()
        str_results = pd.concat([str_results, result]) # Append the result to the DataFrame
    st.write(str_results.sort_values(by='HEAD_STRIKES_ABSORED', ascending=False))   


#with st.expander("Real UFC fans ONLY",expanded=False):
#    audio_file = open('song.mp3', 'rb')
#    audio_bytes = audio_file.read()
#    st.audio(audio_bytes, format='audio/ogg')        

st.code('This application uses data from Greco1899''s scraper of UFC Fight Stats - "https://github.com/Greco1899/scrape_ufc_stats"')


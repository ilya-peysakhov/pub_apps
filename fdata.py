import streamlit as st
import pandas as pd
import time
import polars as pl
import duckdb


#additions
#TD % defense
#strikes landed per minute over time with % over time
#strikes absored per fight
#KOs, TKOs KDs taken

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


st.set_page_config(page_icon="👊", page_title="UFC Data Explorer v0.3", layout="wide")

########start of app
st.header('UFC Fight Stats data explorer')
st.write('This pulls data from Greco1899''s scraper of UFC Fight Stats - https://github.com/Greco1899/scrape_ufc_stats')


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
                              from fs """)
frd = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_details.csv")
ft = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_tott.csv")
fighters= duckdb.sql("SELECT trim(FIGHTER) as FIGHTER,HEIGHT,WEIGHT,REACH,STANCE,DOB,FIRST,LAST,NICKNAME,frd.URL from ft inner join frd on frd.URL = ft.URL")
########################
                      


#
if view =='Fighter One Sheet':
    fighter_list = duckdb.sql("SELECT FIGHTER from fighters  where DOB is not null group by 1 order by 1").df()
    fighter_filter = st.selectbox('Pick a fighter',options=fighter_list)
    fights = duckdb.sql("SELECT BOUT from fr_cleaned where FIGHTER1 = '{}' or FIGHTER2='{}'".format(fighter_filter,fighter_filter))

    winloss = duckdb.sql("SELECT case when FIGHTER1 = '{}' then FIGHTER1_OUTCOME else FIGHTER2_OUTCOME end result from fr_cleaned where FIGHTER1 = '{}' or FIGHTER2='{}' ".format(fighter_filter,fighter_filter,fighter_filter))
    last_fight= duckdb.sql("SELECT left(max(date),10) max_date, left( current_date() - max(date),10) days_since from fr_cleaned where FIGHTER1= '{}' or FIGHTER2='{}' ".format(fighter_filter,fighter_filter)).df()
    fighter_stats = duckdb.sql("SELECT * from fs_cleaned where BOUT in (select BOUT from fights) and FIGHTER ='{}' ".format(fighter_filter))
    sig_str = duckdb.sql("SELECT sum(sig_str_l::INTEGER) s from fighter_stats").df()
    head_str = duckdb.sql("SELECT sum(head_str_l::INTEGER) s from fighter_stats").df()
    td = duckdb.sql("SELECT sum(td_l::INTEGER) s from fighter_stats").df()
    td_rate = duckdb.sql("SELECT round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2) s from fighter_stats").df()
    kd = duckdb.sql("SELECT sum(kd::INTEGER) s from fighter_stats").df()
    ko_wins = duckdb.sql("SELECT count(*) as s from fr_cleaned where ((FIGHTER1='{}' and FIGHTER1_OUTCOME='W') OR (FIGHTER2='{}' and FIGHTER2_OUTCOME='W')) and METHOD='KO/TKO' ").format(fighter_filter,fighter_filter).df()
    
    opp_stats = duckdb.sql("SELECT * from fs_cleaned where BOUT in (select * from fights) and FIGHTER !='{}' ".format(fighter_filter))
    sig_abs = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as s from opp_stats").df()
    head_abs= duckdb.sql("SELECT sum(head_str_l::INTEGER) as s from opp_stats").df()
    td_abs = duckdb.sql("SELECT sum(td_l::INTEGER) as s from opp_stats").df()
    td_abs_rate = duckdb.sql("SELECT round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2) s from opp_stats").df()
    kd_abs = duckdb.sql("SELECT sum(kd::INTEGER) s from opp_stats").df()
    ko_losses = duckdb.sql("SELECT count(*) as s from fr_cleaned where ((FIGHTER1='{}' and FIGHTER1_OUTCOME='L') OR (FIGHTER2='{}' and FIGHTER2_OUTCOME='L')) and METHOD='KO/TKO' ").format(fighter_filter,fighter_filter).df()

    if fighter_filter:
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader('Highlights')
            st.write('Total UFC Fights - '+str(len(fights.df())))
            st.write(str(len(duckdb.sql("SELECT * from winloss where result='W'").df()))+' Wins'+' / '+str(len(duckdb.sql("SELECT * from winloss where result='L'").df()))+' Losses')
            if len(fights.df()) >0:
                st.write('Latest fight - '+str(last_fight['max_date'].values[0])+' - '+str(last_fight['days_since'].values[0])+ ' ago')
            st.write(str(int(ko_wins['s'].sum()))+' KO/TKO Wins')
            st.write(str(int(ko_losses['s'].sum()))+' KO/TKO Losses')
        with col2:
            st.subheader('Striking')
            st.write(str(int(sig_abs['s'].sum()))+' Significant Strikes Absored')
            st.write(str(int(head_abs['s'].sum()))+' Head Strikes Absored')
            st.write(str(int(sig_str['s'].sum()))+' Significant Strikes Landed')
            st.write(str(int(head_str['s'].sum()))+' Head Strikes Landed')
            st.write(str(int(kd['s'].sum()))+' Knockdowns Landed')
            st.write(str(int(kd_abs['s'].sum()))+' Knockdowns Absored')
        with col3:
            st.subheader('Grappling/Wrestling')
            st.write(str(int(td['s'].sum()))+' Total Takedowns Landed'+' at a rate of '+"{:.0%}".format(td_rate['s'].sum()) )
            st.write(str(int(td_abs['s'].sum()))+' Total Takedowns Given Up'+' at a rate of '+"{:.0%}".format(td_abs_rate['s'].sum()) )
        
        st.subheader('Fight Results')
        st.write(duckdb.sql("SELECT * EXCLUDE (DATE,BOUT,WEIGHTCLASS,TIME_FORMAT,URL),left(DATE,10) date from fr_cleaned where FIGHTER1= '{}' or FIGHTER2='{}' order by date desc".format(fighter_filter,fighter_filter)).df())

    bout_filter = st.selectbox('Pick a bout',options=fights.df().drop_duplicates())

    if bout_filter:
        st.subheader('Bout Stats')
        st.write(duckdb.sql("SELECT * EXCLUDE (BOUT) from fighter_stats where BOUT ='{}' ".format(bout_filter)).df())

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



with st.expander("Surprise and Delight",expanded=False):
    audio_file = open('song.mp3', 'rb')
    audio_bytes = audio_file.read()
    st.audio(audio_bytes, format='audio/ogg')        



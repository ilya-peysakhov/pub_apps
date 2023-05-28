import streamlit as st
import pandas as pd
import time
import polars as pl
import duckdb
import datetime
import altair as alt


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


st.set_page_config(page_icon="👊", page_title="UFC Stats Explorer v0.9", layout="wide")

########start of app
st.header('UFC Fight Stats explorer')

view = st.sidebar.radio('Select a view',('Fighter One Sheet','All Time Stats','Show all dataset samples'))


###################### data pull and clean

ed = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_event_details.csv")
ed_c = duckdb.sql("SELECT TRIM(EVENT) as EVENT, strptime(DATE, '%B %d, %Y') as  DATE, URL, LOCATION FROM ed")
fd = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_details.csv")
fed = duckdb.sql("SELECT TRIM(fd.EVENT) as EVENT, TRIM(fd.BOUT) as BOUT, fd.URL, DATE,LOCATION from ed_c inner join fd on ed_c.EVENT=fd.EVENT ")
fr = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_results.csv")
fr = fr.to_pandas()
fr['EVENT'] = fr['EVENT'].str.replace("'", "") 
fr['BOUT'] = fr['BOUT'].str.replace("'", "")
fr = pl.from_pandas(fr)
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
fs = fs.to_pandas()
fs['FIGHTER'] = fs['FIGHTER'].str.replace("'", "") 
fs['BOUT'] = fs['BOUT'].str.replace("'", "") 
fs = pl.from_pandas(fs)
fs_cleaned = duckdb.sql("""SELECT fs.EVENT,replace(trim(BOUT),'  ',' ') as BOUT,ROUND, trim(FIGHTER) as FIGHTER,KD,
                              split_part("SIG.STR.",' of ',1) sig_str_l,
                              split_part("SIG.STR.",' of ',2) sig_str_a,
                              split_part("TOTAL STR.",' of ',1) total_str_l,
                              split_part("TOTAL STR.",' of ',2) total_str_a,
                              split_part(TD,' of ',1) td_l,
                              split_part(TD,' of ',2) td_a,
                              "SUB.ATT","REV.",CTRL,
                              split_part(HEAD,' of ',1) head_str_l,
                              split_part(HEAD,' of ',2) head_str_a,
                              split_part(LEG,' of ',1) leg_str_l,
                              split_part(LEG,' of ',2) leg_str_a,
                              DATE
                              from fs 
                              left join ed_c on ed_c.EVENT = fs.EVENT
                              WHERE FIGHTER IS NOT NULL """)
frd = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_details.csv")
ft = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_tott.csv")

ft = ft.to_pandas()
ft['FIGHTER'] = ft['FIGHTER'].str.replace("'", "") 

fighters= duckdb.sql("SELECT trim(FIGHTER) as FIGHTER,HEIGHT,WEIGHT,REACH,STANCE,DOB,FIRST,LAST,NICKNAME,frd.URL from ft inner join frd on frd.URL = ft.URL")
########################
                      


#
if view =='Fighter One Sheet':
    st.text('Display all relevant fighter stats in just 1 click. Choose your fighter below to get started')
    fighter_list = duckdb.sql("SELECT FIGHTER from fighters  where length(DOB) >3 group by 1 order by 1").df()
    f1, f2 = st.columns(2)
    with f1:
        fighter_filter = st.selectbox('Pick a fighter',options=fighter_list)
    with f2:
        st.radio("Analysis Length (under development)",('Career','Last 3 fights'),disabled=True)
    st.divider()
    fights = duckdb.sql(f"SELECT BOUT from fr_cleaned where FIGHTER1 = '{fighter_filter}' or FIGHTER2='{fighter_filter}'")

    winloss = duckdb.sql(f"SELECT case when FIGHTER1 = '{fighter_filter}' then FIGHTER1_OUTCOME else FIGHTER2_OUTCOME end result from fr_cleaned where FIGHTER1 = '{fighter_filter}' or FIGHTER2='{fighter_filter}' ")
    last_fight= duckdb.sql(f"SELECT left(max(date),10) max_date, left( current_date() - max(date),10) days_since from fr_cleaned where FIGHTER1= '{fighter_filter}' or FIGHTER2='{fighter_filter}' ").df()
    fighter_stats = duckdb.sql(f"SELECT * from fs_cleaned where BOUT in (select BOUT from fights) and FIGHTER ='{fighter_filter}' ")
    cleaned_fighter_stats = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_str, sum(head_str_l::INTEGER) as head_str, sum(td_l::INTEGER) as td_l, round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2)  as td_rate, sum(kd::INTEGER) as kd, from fighter_stats").df()
    ko_wins = duckdb.sql(f"SELECT count(*) as s from fr_cleaned where ((FIGHTER1='{fighter_filter}' and FIGHTER1_OUTCOME='W') OR (FIGHTER2='{fighter_filter}' and FIGHTER2_OUTCOME='W')) and trim(METHOD)='KO/TKO' ").df()
    
    opp_stats = duckdb.sql(f"SELECT * from fs_cleaned where BOUT in (select * from fights) and FIGHTER !='{fighter_filter}' ")
    cleaned_opp_stats = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_abs ,sum(head_str_l::INTEGER) as head_abs,sum(head_str_a::INTEGER) as head_at,sum(td_l::INTEGER) as td_abs,round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2) as td_abs_rate,sum(kd::INTEGER) as kd_abs from opp_stats").df()
    ko_losses = duckdb.sql(f"SELECT count(*) as s from fr_cleaned where ((FIGHTER1='{fighter_filter}' and FIGHTER1_OUTCOME='L') OR (FIGHTER2='{fighter_filter}' and FIGHTER2_OUTCOME='L')) and trim(METHOD)='KO/TKO' ").df()


    if fighter_filter:
        col1,col2,col3,col4,col5 = st.columns(5)
        with col1:
            st.subheader('Bio')
            st.metric('Height',value=str(duckdb.sql(f"SELECT HEIGHT FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]))
            st.metric('Division',value=str(duckdb.sql(f"SELECT WEIGHT FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]))
            st.metric('Reach',value=str(duckdb.sql(f"SELECT REACH FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]))
            
            dob_str = str(duckdb.sql(f"SELECT DOB FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0])
            dob = datetime.datetime.strptime(dob_str, '%b %d, %Y')
            age = datetime.datetime.now() - dob
            age_years = age.days // 365
            st.metric('Age',value=age_years, delta =dob_str, delta_color="off")
            if len(fights.df()) >0:
                st.metric('Last fought', delta=str(last_fight['max_date'].values[0]),value=str(last_fight['days_since'].values[0]))


        with col2:
            st.subheader('Highlights')
            
            w1,w2 = st.columns(2)
            with w1:
                st.metric('UFC Fights',value=len(fights.df()) )
                st.metric('Rounds',value=fighter_stats.shape[0] )
            with w2:
                st.metric('Wins',value=len(duckdb.sql("SELECT * from winloss where result='W'").df()) )
                st.metric('Losses',value=len(duckdb.sql("SELECT * from winloss where result='L'").df()) )
            st.metric('KO/TKO Wins', value = int(ko_wins['s']) )
            st.metric('KO/TKO Losses', value = int(ko_losses['s']))
        with col3:
            st.subheader('Striking')
            st.metric('Significant Strikes Absored',value=int(cleaned_opp_stats['sig_abs']))
            st.metric('Head Strikes Absored',value=int(cleaned_opp_stats['head_abs']))
            st.metric('Significant Strikes Landed',value=int(cleaned_fighter_stats['sig_str']))
            st.metric('Head Strikes Landed',value=int(cleaned_fighter_stats['head_str']))
            st.metric('Knockdowns Landed',value=int(cleaned_fighter_stats['kd']))
            st.metric('Knockdowns Absored',value=int(cleaned_opp_stats['kd_abs']))
        with col4:
            st.subheader('Wrestling')
            st.metric('Total Takedowns Landed',value=int(cleaned_fighter_stats['td_l']),delta="{0:.0%}".format(round(float(cleaned_fighter_stats['td_rate']),2)), delta_color="normal")
            st.metric('Total Takedowns Given Up',value=int(cleaned_opp_stats['td_abs']),delta="{0:.0%}".format(round(float(cleaned_opp_stats['td_abs_rate']),2)), delta_color="inverse")
        with col5:
            st.subheader('Advanced Stats')
            st.metric('Significant Strikes Differential',value=round(float(cleaned_fighter_stats['sig_str']/cleaned_opp_stats['sig_abs']),1))
            st.metric('Head Strikes Differential',value=round(float(cleaned_fighter_stats['head_str']/cleaned_opp_stats['head_abs']),1))
            st.metric('Power Differential (Knockdowns)',value=round(float(cleaned_fighter_stats['kd']/cleaned_opp_stats['kd_abs']),1))
            st.metric('Takedown Differential',value=round(float(cleaned_fighter_stats['td_l']/cleaned_opp_stats['td_abs']),1))
            st.caption('opponent head str %')
            st.metric('Head Movement',value=round(float(cleaned_opp_stats['head_abs']/cleaned_opp_stats['head_at']),2))
        c1, c2 = st.columns(2)
        with c1:
            st.write("Strikes Attempted")
            str_a = duckdb.sql(f"SELECT DATE, sum(total_str_a::INT) as Total_Strikes_At from fighter_stats group by 1").df()
            str_a_chart = alt.Chart(str_a).mark_bar().encode(x='DATE',y='Total_Strikes_At')
            st.altair_chart(str_a_chart, theme="streamlit")
            st.write("Net Sig Strike Landed difference")
            str_dif = duckdb.sql(f"SELECT a.DATE, sum(a.sig_str_l::INT)-sum(b.sig_str_l::INT) as Strike_Diff from fighter_stats as a inner join opp_stats as b on a.DATE = b.DATE and a.BOUT=b.BOUT and a.ROUND=b.ROUND group by 1").df()
            str_dif_chart = alt.Chart(str_dif).mark_bar().encode(x='DATE',y='Strike_Diff')
            st.altair_chart(str_dif_chart, theme="streamlit")
              
        with c2:
            st.write("Takedowns Attempted")
            td_a = duckdb.sql(f"SELECT DATE,  sum(td_a::int) TD_At from fighter_stats group by 1").df()
            td_a_chart = alt.Chart(td_a).mark_bar().encode(x='DATE',y='TD_At')
            st.altair_chart(td_a_chart, theme="streamlit")
            st.write("Net Takedown difference")
            td_dif = duckdb.sql(f"SELECT a.DATE, sum(a.td_a::INT)-sum(b.td_a::INT) as TD_At_Diff from fighter_stats as a inner join opp_stats as b on a.DATE = b.DATE and a.BOUT=b.BOUT and a.ROUND=b.ROUND group by 1").df()
            td_dif_chart = alt.Chart(td_dif).mark_bar().encode(x='DATE',y='TD_At_Diff')
            st.altair_chart(td_dif_chart, theme="streamlit")


        st.divider()
        with st.expander("Career Results",expanded=False):
            career_results = duckdb.sql(f"SELECT left(DATE,10) AS DATE ,EVENT,case when FIGHTER1='{fighter_filter}' then FIGHTER2 else FIGHTER1 end as OPPONENT,case when FIGHTER1='{fighter_filter}' then FIGHTER1_OUTCOME else FIGHTER2_OUTCOME end as RESULT,METHOD,ROUND, TIME,DETAILS from fr_cleaned where FIGHTER1= '{fighter_filter}' or FIGHTER2='{fighter_filter}' order by DATE desc").df()
            st.write(career_results.set_index(career_results.columns[0]))
    
    st.divider()
    with st.expander("Single Fight Stats",expanded=False):
        bout_filter = st.selectbox('Pick a bout',options=fights.df().drop_duplicates())
        fight_results = duckdb.sql(f"SELECT * EXCLUDE (BOUT,FIGHTER,EVENT) from fs where replace(trim(BOUT),'  ',' ') ='{bout_filter}'  and trim(FIGHTER)='{fighter_filter}' ").df()
        
        if bout_filter:
            st.write(fight_results.set_index(fight_results.columns[0]).T)

elif view =='Show all dataset samples':
    st.write('Fighter Details (cleaned)')
    st.dataframe(duckdb.sql("select * from fighters limit 5").df())
    st.write('Events & Fights (cleaned)')
    st.write(duckdb.sql("SELECT * from fed limit 5").df())
    st.write('Fight Results (cleaned)')
    st.write(duckdb.sql("SELECT * from fr_cleaned limit 5").df())
    st.write('Fight Stats')
    st.write(duckdb.sql("SELECT * from fs limit 5").df())
    st.write("Data Check - Events without data")
    anomalies = duckdb.sql("select left(DATE,10) as DATE,ed_c.EVENT, count(BOUT) as bouts_with_stats from ed_c left join fs on ed_c.EVENT =fs.EVENT group by 1,2 having bouts_with_stats=0 order by 1 desc").df()
    st.write(anomalies.set_index(anomalies.columns[0]))
else:
    st.subheader('Lifetime stats unless otherwise noted (last 2 years)')
    c1, c2  = st.columns(2)
    with c1:
        st.write("Fights by month")
        fights_monthly= duckdb.sql("SELECT date_trunc('month',date) as MONTH,count(*) as FIGHTS from fed group by 1 order by 1 asc").df()
        fights_monthly_chart = alt.Chart(fights_monthly).mark_bar().encode(x='MONTH',y='FIGHTS')
        st.altair_chart(fights_monthly_chart, theme="streamlit")
        
        st.divider()

        st.write('Most experienced referees (2yr)')
        refs = duckdb.sql("SELECT REFEREE,count(*) fights from fr_cleaned where date between current_date() -730 and current_date() group by 1 order by 2 desc limit 10").df()
        st.dataframe(refs.set_index(refs.columns[0]),use_container_width=False)
        st.divider()
        st.write("Fights by result method (2yr)")
        methods = duckdb.sql("SELECT method, count(*) FIGHTS from fr_cleaned where date between current_date() -730 and current_date() group by 1 ").df()
        method_chart = alt.Chart(methods).mark_arc(stroke='black', strokeWidth=0.3).encode(
                theta="FIGHTS",
                color=alt.Color('METHOD', scale=alt.Scale(scheme='blueorange'))
            )
        st.write(method_chart)

        
    
    with c2:
        st.write("Number of Fights per Fighter")
        fight_distro = duckdb.sql("select FIGHTS,count(1) FIGHTERS from (select FIGHTER,COUNT(DISTINCT EVENT||BOUT) FIGHTS from fs_cleaned group by 1) group by 1 order by 1").df()
        fight_distro_chart = alt.Chart(fight_distro).mark_bar().encode(x='FIGHTS',y='FIGHTERS')
        st.altair_chart(fight_distro_chart, theme="streamlit")
        
        st.divider()
        
        st.write('Most commonly used venues (2yr)')
        locations = duckdb.sql("SELECT LOCATION,count(distinct EVENT) EVENTS from fed where date between current_date() -730 and current_date() group by 1 order by 2 desc limit 10").df()
        # st.dataframe(locations.set_index(locations.columns[0]),use_container_width=True)
        location_chart = alt.Chart(locations).mark_bar().encode(
            x=alt.X('EVENTS:Q'),
            y=alt.Y('LOCATION:O', sort='-x')
        )
        st.write(location_chart)

        st.divider()

        st.metric('Number of Fighters fought (2yr)',value=str(duckdb.sql("""SELECT count(distinct fighter) s from 
            (SELECT FIGHTER1 fighter from fr_cleaned where date between current_date() -730 and current_date() group by 1 
            UNION 
            SELECT FIGHTER2 fighter from fr_cleaned where date between current_date() -730 and current_date() group by 1)
            """).df().iloc[0,0]))

    st.write("Method of winning as a percentage of all methods over time")
    frame = st.selectbox('Pick a time dimension',['year','quarter','month','week','day'])
    methods_over_time = duckdb.sql(f"SELECT case when METHOD like 'Decision%' then 'Decision' else METHOD end as METHOD, date_trunc('{frame}',date) as MONTH, count(*)/sum(sum(1)) over (partition by MONTH) METHOD_PCT from fr_cleaned  group by 1,2 ").df()
    methods_over_time_chart = alt.Chart(methods_over_time).mark_area(stroke='black', strokeWidth=0.3).encode(
            x="MONTH:T",
            y="METHOD_PCT:Q",
            color=alt.Color('METHOD', scale=alt.Scale(scheme='blueorange'))
            )
        
    st.write(methods_over_time_chart.properties(width=1100, height=500))
    min_fights = st.number_input('Minimum Fights',step=1,value=5)
    st.write(f"Minimum {min_fights} fights, historical rankings for total career offensive and defensive stats")
    fs_cleaned = fs_cleaned.pl()
    fighters = duckdb.sql(f"SELECT fighter FROM fs_cleaned GROUP BY 1 having count(distinct BOUT||EVENT) >={min_fights} ").df()
    str_results = pd.DataFrame()
    for f in fighters['FIGHTER']:
        query = f"SELECT '{f}' AS FIGHTER, SUM(head_str_l::INTEGER) AS HEAD_STRIKES_ABSORED, SUM(head_str_a::INTEGER) AS HEAD_STRIKES_AT ,SUM(sig_str_l::INTEGER) AS SIG_STRIKES_ABSORBED,SUM(leg_str_l::INTEGER) as LEG_STRIKES_ABSORBED,sum(KD::INTEGER) as KD_ABSORED, sum(TD_L::INT) as TD_GIVEN_UP FROM fs_cleaned WHERE BOUT LIKE '%{f}%' AND fighter != '{f}' "
        result = duckdb.sql(query).df()
        str_results = pd.concat([str_results, result]) # Append the result to the DataFrame
    all_time_offense = duckdb.sql(f"SELECT FIGHTER, COUNT(DISTINCT BOUT||EVENT) as FIGHTS, COUNT(*) AS ROUNDS,  ROUND(ROUNDS/CAST(FIGHTS as REAL),1) as ROUNDS_PER_FIGHT ,SUM(head_str_l::INTEGER) AS HEAD_STRIKES_LANDED, SUM(leg_str_l::INTEGER) as LEG_STRIKES_LANDED,sum(sig_str_l::INTEGER) as SIG_STRIKES_LANDED,sum(KD::INTEGER) as KD_LANDED, sum(TD_L::INT) as TD_LANDED from fs_cleaned group by 1 having FIGHTS>={min_fights}")
    combined_stats = duckdb.sql("SELECT a.*,ROUND(SIG_STRIKES_LANDED/SIG_STRIKES_ABSORBED,1) as SIG_STR_DIFF,ROUND(FLOAT(HEAD_STRIKES_ABSORED/HEAD_STRIKES_AT),2) as HEAD_MOVEMENT, b.* EXCLUDE (FIGHTER) from all_time_offense as a left join str_results as b on a.FIGHTER=b.FIGHTER").df()
    st.write(combined_stats.set_index(combined_stats.columns[0]).sort_values(by='FIGHTS', ascending=False))   

       
       
    #with st.expander("Real UFC fans ONLY",expanded=False):
    #    audio_file = open('song.mp3', 'rb')
    #    audio_bytes = audio_file.read()
    #    st.audio(audio_bytes, format='audio/ogg')        

st.code('This application uses data from Greco1899''s scraper of UFC Fight Stats - "https://github.com/Greco1899/scrape_ufc_stats"')

st.code('Recent changes - new charts in All Time section, fixed stats for fighters with apostraphes,sped up all time rankings to 5s, add leg strikes, all data recovered by Greco1899' )

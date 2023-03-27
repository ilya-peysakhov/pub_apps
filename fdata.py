import streamlit as st
import pandas as pd
import time
import numpy as np
import polars as pl
import duckdb



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
st.header('REWRITING WITH POLARS/DUCKDB!!!')

audio_file = open('song.mp3', 'rb')
audio_bytes = audio_file.read()

st.audio(audio_bytes, format='audio/ogg')

st.header('UFC Fight Stats data explorer')
st.write('This pulls data from Greco1899''s scraper of UFC Fight Stats - https://github.com/Greco1899/scrape_ufc_stats')
st.image('https://media.tenor.com/3igI9osXP0UAAAAM/just-bleed.gif',width=200)

view = st.sidebar.radio('Select a view',('Single Fighter Stats','All Time Stats','Show all dataset samples'))


###################### data pull and clean

ed = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_event_details.csv")
ed_c = duckdb.sql("SELECT TRIM(EVENT) as EVENT, strptime(DATE, '%B %d, %Y') as  DATE, URL, LOCATION FROM ed")
fd = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_details.csv")
fed = duckdb.sql("SELECT TRIM(fd.EVENT) as EVENT, TRIM(fd.BOUT) as BOUT, fd.URL, DATE,LOCATION from ed_c inner join fd on ed_c.EVENT=fd.EVENT ")
fr = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_results.csv")
fr_df = duckdb.sql("""SELECT trim(fr.EVENT) as EVENT, 
                             trim(fr.BOUT) as BOUT, 
                            trim(split_part(fr.BOUT, ' vs. ' ,1)) as FIGHTER1,
                            trim(split_part(fr.BOUT, ' vs. ', 2)) as FIGHTER2,
                            split_part(OUTCOME, '/' ,1) as FIGHTER1_OUTCOME,
                            split_part(OUTCOME, '/', 2) as FIGHTER2_OUTCOME,
                            WEIGHTCLASS,METHOD,ROUND,TIME,left("TIME FORMAT",1) as TIME_FORMAT,REFEREE,DETAILS,fr.URL,date 
                        from fr
                        left join fed on fed.URL = fr.URL""")
fs = pl.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_stats.csv")
cleaned_fs_df = duckdb.sql("""SELECT EVENT,BOUT,ROUND,FIGHTER,KD,
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
fighters= duckdb.sql("SELECT FIGHTER,HEIGHT,WEIGHT,REACH,STANCE,DOB,FIRST,LAST,NICKNAME,frd.URL from ft inner join frd on frd.URL = ft.URL")
########################
                      



#
if view =='Single Fighter Stats':
    fighter_list = duckdb.sql("SELECT FIGHTER from fighters group by 1 order by 1").df()
    fighter_filter = st.selectbox('Pick a fighter',options=fighter_list)
    fights = duckdb.sql("select BOUT from fr_df where FIGHTER1 = '{}' or FIGHTER2='{}'".format(fighter_filter,fighter_filter))
    bouts = fight_stats[fight_stats['BOUT'].str.contains(fighter_filter, case=False)]
    opp_stats = fight_stats[(fight_stats['BOUT'].isin(bouts['BOUT'])) & (fight_stats['FIGHTER']!=fighter_filter)]
    fighter_stats = fight_stats[(fight_stats['BOUT'].isin(bouts['BOUT'])) & (fight_stats['FIGHTER']==fighter_filter)]
    wins = len(fight_results[(fight_results['OUTCOME_1'] == 'W') & (fight_results['FIGHTER_1'] == fighter_filter) | (fight_results['OUTCOME_2'] == 'W') & (fight_results['FIGHTER_2'] == fighter_filter)])
    losses = len(fight_results[(fight_results['OUTCOME_1'] == 'L') & (fight_results['FIGHTER_1'] == fighter_filter) | (fight_results['OUTCOME_2'] == 'L') & (fight_results['FIGHTER_2'] == fighter_filter)])
    
    if fighter_filter:
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader('Total UFC Fights - '+str(fights.count()))
            st.subheader(str(wins)+' Wins')
            st.subheader(str(losses)+' Losses')
            last_fight= duckdb.sql("select max(date) max_date from fr_clean where FIGHTER1= '{}' or FIGHTER2='{}' ".format(fighter_filter,fighter_filter)).collect()
            if fights.count()>0:
                st.write('Last Fight - '+str(last_fight[0]["max_date"].strftime("%Y-%m-%d")))
        with col2:
            st.subheader(str(opp_stats['SIG_STR'].sum())+' Total Career Significant Strikes Absored')
            st.subheader(str(opp_stats['HEAD_STR'].sum())+' Total Career Head Strikes Absored')
        with col3:
            st.subheader(str(fighter_stats['SIG_STR'].sum())+' Total Career Significant Strikes Landed')
        
        st.write('Fight Results')
        st.write(duckdb.sql("select * from fr_clean where FIGHTER1= '{}' or FIGHTER2='{}' ".format(fighter_filter,fighter_filter)))

    bout_filter = st.selectbox('Pick a bout',options=bouts['BOUT'].drop_duplicates())

    if bout_filter:
        st.write(fight_stats[(fight_stats['BOUT']==bout_filter) & (fight_stats['FIGHTER']==fighter_filter)])

elif view =='Show all dataset samples':
    st.write('Fighter Details (cleaned)')
    st.dataframe(duckdb.sql("SELECT * from fighters order by FIGHTER asc limit 5").df())
    st.write('Events & Fights (cleaned)')
    st.write(duckdb.sql("select * from fed limit 5").df())
    st.write('Fight Results (cleaned)')
    st.write(duckdb.sql("select * from fr_df limit 5").df())
    st.write('Fight Stats')
    st.write(duckdb.sql("select * from cleaned_fs_df limit 5").df())    
else:
    c1, c2 = st.columns(2)
    with c1:
        st.write("Fights by month")
        st.area_chart(duckdb.sql("select date_trunc('month',date) date,count(*) fights from fed group by 1 order by 1 asc").toPandas().set_index("date"))
        st.write('Fighters fought in the last 730 days (2 years)')
        st.write(duckdb.sql("""
                       select count(distinct fighter) from 
                        (select FIGHTER1 fighter from fr_clean where date between current_date() -730 and current_date() group by 1 
                        UNION 
                        select FIGHTER2 fighter from fr_clean where date between current_date() -730 and current_date() group by 1)
                        """))
    
    with c2:
        st.write("Events by month")
        st.area_chart(duckdb.sql("select date_trunc('month', date) date, count(distinct EVENT) events from fed group by 1 order by 1 asc").toPandas().set_index("date"))
        st.write('Most experienced referees in the last 2 years')
        st.write(duckdb.sql("select REFEREE,count(*) fights from fr_clean where date between current_date() -730 and current_date() group by 1 order by 2 desc limit 10"))



#st.write(fight_stats.sort_values('SIG_STR', ascending=False))

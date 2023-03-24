import streamlit as st
import pandas as pd
import time
import numpy as np
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles



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


st.set_page_config(page_icon="👊", page_title="UFC Data Explorer v0.2", layout="wide")
spark = SparkSession.builder.getOrCreate()

#event details
ed_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_event_details.csv"
spark.sparkContext.addFile(ed_url)
ed_df = spark.read.csv(SparkFiles.get('ufc_event_details.csv'), header=True)
ed_df.createOrReplaceTempView("ed")
ed_clean_df = spark.sql("select trim(EVENT) EVENT,URL,to_date(DATE,'MMMM d, yyyy') DATE,LOCATION from ed")
ed_clean_df.createOrReplaceTempView("ed_clean")

#fight details
fd_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_details.csv"
spark.sparkContext.addFile(fd_url)
fd_df = spark.read.csv(SparkFiles.get('ufc_fight_details.csv'), header=True)
fd_df.createOrReplaceTempView("fd")
#event + fight details
fed_df = spark.sql("select fd.*, date,LOCATION from ed_clean inner join fd on ed_clean.EVENT=fd.EVENT")
fed_df.createOrReplaceTempView("fed")

#fight results
fr_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_results.csv"
spark.sparkContext.addFile(fr_url)
fr_df = spark.read.csv(SparkFiles.get('ufc_fight_results.csv'), header=True)
fr_df.createOrReplaceTempView("fr")
fr_df = spark.sql("""select trim(fr.EVENT) EVENT, fr.BOUT, 
                    split(fr.BOUT,' vs. ')[0] FIGHTER1,
                    split(fr.BOUT,' vs. ')[1] FIGHTER2,
                    split(OUTCOME,'/')[0] FIGHTER1_OUTCOME,
                    split(OUTCOME,'/')[1] FIGHTER2_OUTCOME,
                    WEIGHTCLASS,METHOD,ROUND,TIME,left(`TIME FORMAT`,1) TIME_FORMAT,REFEREE,DETAILS,fr.URL,date 
                    from fr
                    left join fed on fed.EVENT = trim(fr.EVENT) """)
fr_df.createOrReplaceTempView("fr_clean")

#fight stats
fs_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_stats.csv"
spark.sparkContext.addFile(fs_url)
fs_df = spark.read.csv(SparkFiles.get('ufc_fight_stats.csv'), header=True)
fs_df.createOrReplaceTempView("fs")

#cleanup fight stats
cleaned_fs_df = spark.sql("""select EVENT,BOUT,ROUND,FIGHTER,KD,
                          split(`SIG.STR.`,' of ')[0] sig_str_l,
                          split(`SIG.STR.`,' of ')[1] sig_str_a,
                          split(`TOTAL STR.`,' of ')[0] total_str_l,
                          split(`TOTAL STR.`,' of ')[1] total_str_a,
                          split(TD,' of ')[0] td_l,
                          split(TD,' of ')[1] td_a,
                          `SUB.ATT`,`REV.`,CTRL,
                          split(HEAD,' of ')[0] head_str_l,
                          split(HEAD,' of ')[1] head_str_a  
                          from fs limit 5""")



frd_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_details.csv"
spark.sparkContext.addFile(frd_url)
frd_df = spark.read.csv(SparkFiles.get('ufc_fighter_details.csv'), header=True)
frd_df.createOrReplaceTempView("frd")

ft_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_tott.csv"
spark.sparkContext.addFile(ft_url)
ft_df = spark.read.csv(SparkFiles.get('ufc_fighter_tott.csv'), header=True)
ft_df.createOrReplaceTempView("ft")

fighters_df = spark.sql("select FIGHTER,HEIGHT,WEIGHT,REACH,STANCE,DOB,FIRST,LAST,NICKNAME,frd.URL from ft inner join frd on frd.URL = ft.URL")
fighters_df.createOrReplaceTempView("fighters")

st.header('REWRITING WITH PYSPARK!!!')

audio_file = open('song.mp3', 'rb')
audio_bytes = audio_file.read()

st.audio(audio_bytes, format='audio/ogg')

st.header('UFC Fight Stats data explorer')
st.write('This pulls data from Greco1899''s scraper of UFC Fight Stats - https://github.com/Greco1899/scrape_ufc_stats')
st.image('https://media.tenor.com/3igI9osXP0UAAAAM/just-bleed.gif',width=200)

view = st.sidebar.radio('Select a view',('Single Fighter Stats','All Time Stats','Show all data'))


@st.cache_data
def refreshData():
    events = pd.read_csv('https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_event_details.csv')
    fight_details = pd.read_csv('https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_details.csv')
    fight_results = pd.read_csv('https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_results.csv')
    fight_stats = pd.read_csv('https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_stats.csv')
    fighter_details = pd.read_csv('https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_details.csv')
    fighter_tot = pd.read_csv('https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_tott.csv')
    return events,fight_details,fight_results,fight_stats,fighter_details,fighter_tot

alldata = refreshData()
events = alldata[0]
fight_details = alldata[1]
fight_results = alldata[2]
fight_stats = alldata[3]
fighter_details = alldata[4]
fighter_tot = alldata[5]

#transforms
fighter_merged = fighter_details.merge(fighter_tot, on='URL')
fight_stats[['SIG_STR', 'SIG_STR_ATTEMPTED']] = fight_stats['SIG.STR.'].str.split(' of ', expand=True)
fight_stats['SIG_STR'] = fight_stats['SIG_STR'].fillna('0').str.replace('\D+', '').astype(int)
fight_stats = fight_stats.drop('SIG.STR.', axis=1)
fight_stats[['HEAD_STR', 'HEAD_STR_ATTEMPTED']] = fight_stats['HEAD'].str.split(' of ', expand=True)
fight_stats['HEAD_STR'] = fight_stats['HEAD_STR'].fillna('0').str.replace('\D+', '').astype(int)
fight_stats = fight_stats.drop('HEAD', axis=1)

fight_results[['OUTCOME_1', 'OUTCOME_2']] = fight_results['OUTCOME'].str.split('/', expand=True)
fight_results[['FIGHTER_1', 'FIGHTER_2']] = fight_results['BOUT'].str.split('  vs. ', expand=True)
fight_results['FIGHTER_2'] = fight_results['FIGHTER_2'].str.strip()

fight_results = fight_results.drop('OUTCOME',axis=1)

#
if view =='Single Fighter Stats':
    fighter_list = list(fighters_df.select('FIGHTER').toPandas()['FIGHTER'])
    fighter_filter = st.selectbox('Pick a fighter',options=fighter_list)
    fightsz = fr_df.filter(FIGHTER==fighter_filter)
    st.write(fightsz)
    fights = fight_results[fight_results['BOUT'].str.contains(fighter_filter,case=False)]
    bouts = fight_stats[fight_stats['BOUT'].str.contains(fighter_filter, case=False)]
    opp_stats = fight_stats[(fight_stats['BOUT'].isin(bouts['BOUT'])) & (fight_stats['FIGHTER']!=fighter_filter)]
    fighter_stats = fight_stats[(fight_stats['BOUT'].isin(bouts['BOUT'])) & (fight_stats['FIGHTER']==fighter_filter)]
    wins = len(fight_results[(fight_results['OUTCOME_1'] == 'W') & (fight_results['FIGHTER_1'] == fighter_filter) | (fight_results['OUTCOME_2'] == 'W') & (fight_results['FIGHTER_2'] == fighter_filter)])
    losses = len(fight_results[(fight_results['OUTCOME_1'] == 'L') & (fight_results['FIGHTER_1'] == fighter_filter) | (fight_results['OUTCOME_2'] == 'L') & (fight_results['FIGHTER_2'] == fighter_filter)])

    if fighter_filter:
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader('Total UFC Fights - '+str(fights.shape[0]))
            st.subheader(str(wins)+' Wins')
            st.subheader(str(losses)+' Losses')
        with col2:
            st.subheader(str(opp_stats['SIG_STR'].sum())+' Total Career Significant Strikes Absored')
            st.subheader(str(opp_stats['HEAD_STR'].sum())+' Total Career Head Strikes Absored')
        with col3:
            st.subheader(str(fighter_stats['SIG_STR'].sum())+' Total Career Significant Strikes Landed')
        st.write('Fight Results')
        st.dataframe(fights, use_container_width=False)

    bout_filter = st.selectbox('Pick a bout',options=bouts['BOUT'].drop_duplicates())

    if bout_filter:
        st.write(fight_stats[(fight_stats['BOUT']==bout_filter) & (fight_stats['FIGHTER']==fighter_filter)])

elif view =='Show all data':
    st.write('Fighter Details')
    st.write(spark.sql("select * from fighters order by FIGHTER asc"))
    st.write('Events & Fights')
    st.write(spark.sql("select * from fed limit 5"))
    st.write('Fight Results')
    st.write(spark.sql("select * from fr_clean limit 5"))
    st.write('Fight Stats')
    st.write(spark.sql("select * from fs limit 5"))    
else:
    st.write("Fights by month")
    st.area_chart(spark.sql("select date_trunc('month',date) date,count(*) fights from fed group by 1 order by 1 asc").toPandas().set_index("date"))
    st.write("Events by month")
    st.area_chart(spark.sql("select date_trunc('month', date) date, count(distinct EVENT) events from fed group by 1 order by 1 asc").toPandas().set_index("date"))
    st.write('Fighters fought in the last 365 days')
    st.write(spark.sql("""
                   select count(distinct fighter) from 
                    (select FIGHTER1 fighter from fr_clean where date between current_date() -365 and current_date() group by 1 
                    UNION 
                    select FIGHTER2 fighter from fr_clean where date between current_date() -365 and current_date() group by 1)
                    """))



#st.write(fight_stats.sort_values('SIG_STR', ascending=False))

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

ed_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_event_details.csv"
spark.sparkContext.addFile(ed_url)
ed_df = spark.read.csv(SparkFiles.get('ufc_event_details.csv'), header=True)
ed_df.createOrReplaceTempView("ed")

fd_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_details.csv"
spark.sparkContext.addFile(fd_url)
fd_df = spark.read.csv(SparkFiles.get('ufc_fight_details.csv'), header=True)
fd_df.createOrReplaceTempView("fd")

fr_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_results.csv"
spark.sparkContext.addFile(fr_url)
fr_df = spark.read.csv(SparkFiles.get('ufc_fight_results.csv'), header=True)
fr_df.createOrReplaceTempView("fr")

fs_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_stats.csv"
spark.sparkContext.addFile(fs_url)
fs_df = spark.read.csv(SparkFiles.get('ufc_fight_stats.csv'), header=True)
fs_df.createOrReplaceTempView("fs")

fd_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_details.csv"
spark.sparkContext.addFile(fd_url)
fd_df = spark.read.csv(SparkFiles.get('ufc_fighter_details.csv'), header=True)
fd_df.createOrReplaceTempView("fd")

ft_url="https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_tott.csv"
spark.sparkContext.addFile(ft_url)
ft_df = spark.read.csv(SparkFiles.get('ufc_fighter_tott.csv'), header=True)
ft_df.createOrReplaceTempView("ft")

fed_df = spark.sql("select fd.*, date, location from fe inner join fd on fe.event=fd.event")
st.write(fed_df.show(5))

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
    fighter_list = fighter_merged['FIGHTER'].tolist()
    fighter_filter = st.selectbox('Pick a fighter',options=fighter_list)
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
    st.write('Events')
    st.write(events.head(5))
    st.write('Fight Details')
    st.write(fight_results.head(5))
    st.write('Fight Stats')
    st.write(fight_stats.head(5))
    st.write('Fighter Details')
    st.write(fighter_merged.head(5))
else:
    st.write("Building")




#st.write(fight_stats.sort_values('SIG_STR', ascending=False))

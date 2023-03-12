import streamlit as st
import pandas as pd
import time
import numpy as np

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


st.set_page_config(page_icon="👊", page_title="UFC Data", layout="wide")

st.header('UFC Fight Stats data explorer')
st.write('This pulls data from Greco1899''s scraper of UFC Fight Stats - https://github.com/Greco1899/scrape_ufc_stats')
st.image('https://media.tenor.com/3igI9osXP0UAAAAM/just-bleed.gif',width=100)

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
fight_stats[['SIG_STR', 'SIG_STR_ATTEMPTED']] = fight_stats['SIG.STR.'].str.split(' of ', expand=True)
fight_stats[['SIG_STR', 'SIG_STR_ATTEMPTED']] = fight_stats['SIG.STR.'].str.split(' of ', expand=True)
fight_stats['SIG_STR'] = fight_stats['SIG_STR'].fillna('0').str.replace('\D+', '').astype(int)

fighter_list = fighter_tot['FIGHTER'].tolist()
fighter_filter = st.selectbox('Pick a fighter',options=fighter_list)

fights = fight_results[fight_results['BOUT'].str.contains(fighter_filter,case=False)]


bouts = fight_stats[fight_stats['BOUT'].str.contains(fighter_filter, case=False)]
opp_stats = fight_stats[(fight_stats['BOUT'].isin(bouts['BOUT'])) & (fight_stats['FIGHTER']!=fighter_filter)]
fighter_stats = fight_stats[(fight_stats['BOUT'].isin(bouts['BOUT'])) & (fight_stats['FIGHTER']==fighter_filter)]


if fighter_filter:
    col1,col2,col3 = st.columns(3)
    with col1:
        st.subheader('Total UFC Fights - '+str(fights.shape[0]))
    with col2:
        st.subheader(str(opp_stats['SIG_STR'].sum())+' Total Career Significant Strikes Absored')
    with col3:
        st.subheader(str(fighter_stats['SIG_STR'].sum())+' Total Career Significant Strikes Landed')
    
    st.write('Fight Results')
    st.write(fights)

bout_filter = st.selectbox('Pick a bout',options=bouts['BOUT'].drop_duplicates())

if bout_filter:
    st.write(fight_stats[(fight_stats['BOUT']==bout_filter) & (fight_stats['FIGHTER']==fighter_filter)])


if st.button("Show available datasets"):
    st.write('Events')
    st.write(events.head(5))
    st.write('Fight Details')
    # st.write(filtered_fight_details.head(5))
    # st.write('Fight Results')
    st.write(fight_results.head(5))
    st.write('Fight Stats')
    st.write(fight_stats.head(5))
    st.write('Fighter Details')
    st.write(fighter_details.head(5))
    st.write('Fighter Tots')
    st.write(fighter_tot.head(5))



#st.write(fight_stats.sort_values('SIG_STR', ascending=False))

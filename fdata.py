import streamlit as st
import pandas as pd
pd.options.mode.copy_on_write = True
import altair as alt

import duckdb
import time
import datetime
# import streamlit_shadcn_ui as ui
from streamlit_ace import st_ace
import plotly.express as px


#additions
#strikes landed per minute over time with % over time
#strikes absored per fight

###################################


st.set_page_config(page_icon="👊", page_title="UFC Stats Explorer v1.0", layout="wide")

########start of app


if st.sidebar.button('STOP'):
  st.stop()

view = st.sidebar.radio('Select a view',('Welcome','Fighter One Sheet','Interesting Stats','Aggregate Table','Show all dataset samples','SQL Editor','Tale of the Tape'))


###################### data pull and clean
@st.cache_data(ttl = '7d')
def getData():
  ed = pd.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_event_details.csv",dtype_backend='pyarrow',engine='pyarrow')
  fd = pd.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_details.csv",dtype_backend='pyarrow',engine='pyarrow')
  fr = pd.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_results.csv",dtype_backend='pyarrow',engine='pyarrow')
  fs = pd.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fight_stats.csv",dtype_backend='pyarrow',engine='pyarrow')
  frd = pd.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_details.csv",dtype_backend='pyarrow',engine='pyarrow')
  ft = pd.read_csv("https://github.com/Greco1899/scrape_ufc_stats/raw/main/ufc_fighter_tott.csv",dtype_backend='pyarrow',engine='pyarrow')
  return ed, fd, fr, fs, frd, ft

alldata = getData()


ed, fd, fr, fs, frd, ft = alldata[0], alldata[1], alldata[2], alldata[3], alldata[4], alldata[5]

@st.cache_data(ttl = '7d')
def cleanData():
  ed_c = duckdb.sql("SELECT TRIM(EVENT) as EVENT, strptime(DATE, '%B %d, %Y') as  DATE, URL, LOCATION FROM ed").df()
  fed = duckdb.sql("SELECT TRIM(fd.EVENT) as EVENT, TRIM(fd.BOUT) as BOUT, fd.URL, DATE,LOCATION from ed_c inner join fd on ed_c.EVENT=fd.EVENT ").df()
  fr["EVENT"] = fr["EVENT"].str.replace("'", "")  # Replace single quotes in EVENT column
  fr["BOUT"] = fr["BOUT"].str.replace("'", "")  # Replace single quotes in BOUT column
  fr_cleaned = duckdb.sql("""SELECT trim(fr.EVENT) as EVENT, 
                               replace(trim(fr.BOUT),'  ',' ') as BOUT, 
                              trim(split_part(fr.BOUT, ' vs. ' ,1)) as FIGHTER1,
                              trim(split_part(fr.BOUT, ' vs. ', 2)) as FIGHTER2,
                              split_part(OUTCOME, '/' ,1) as FIGHTER1_OUTCOME,
                              split_part(OUTCOME, '/', 2) as FIGHTER2_OUTCOME,
                              WEIGHTCLASS,METHOD,ROUND,TIME,left("TIME FORMAT",1) as TIME_FORMAT,REFEREE,DETAILS,fr.URL,date 
                          from fr
                          left join fed on fed.URL = fr.URL""").df()
  fs["FIGHTER"] = fs["FIGHTER"].str.replace("'", "")  # Replace single quotes in EVENT column
  fs["BOUT"] = fs["BOUT"].str.replace("'", "")  # Replace single quotes in BOUT column
  fs_cleaned = duckdb.sql("""SELECT fs.EVENT,replace(trim(BOUT),'  ',' ') as BOUT,ROUND, trim(FIGHTER) as FIGHTER,KD,
                                split_part("SIG.STR."::string,' of ',1) sig_str_l,
                                split_part("SIG.STR."::string,' of ',2) sig_str_a,
                                split_part("TOTAL STR."::string,' of ',1) total_str_l,
                                split_part("TOTAL STR."::string,' of ',2) total_str_a,
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
                                WHERE FIGHTER IS NOT NULL """).df()
  ft["FIGHTER"] = ft["FIGHTER"].str.replace("'", "")  # Replace single quotes in BOUT column
  fighters= duckdb.sql("SELECT trim(FIGHTER) as FIGHTER,HEIGHT,WEIGHT,REACH,STANCE,DOB,FIRST,LAST,NICKNAME,frd.URL from ft inner join frd on frd.URL = ft.URL where dob !='--'").df()
  return fed, fr_cleaned, fs_cleaned, fighters, ed_c

cleandata = cleanData()
fed, fr_cleaned, fs_cleaned, fighters, ed_c = cleandata[0],cleandata[1],cleandata[2],cleandata[3],cleandata[4] 

########################
fighter_list = duckdb.sql("SELECT FIGHTER from fighters  where length(DOB) >3 group by 1 order by 1").df()                      

if view=='Welcome':
  st.title('Welcome to UFC Stats Explorer!👊')
  st.write("""
  The purpose of this application is to make it easy to dive into the world of UFC fight stats. The fight data goes back to 1994 and is available at a round by round level, which allows for very granular analysis.
              
  - To view a summary of a single fighter throughout their career, or a few of their recent fights, head over to the Fighter One Sheet page. 
  
  - To see a high level overview of the UFC over the years, including fights by month, most active referees, fights by division and most common methods of victory, click Interesting Stats
  
  - To view an all time ranking of every UFC fighter with both offensive and defensive stats (such as, which fighter has thrown the most leg kicks ever), go to Aggregate Table.
  
  - If you have your own ideas for the data, head over to SQL Editor, and write your own code. You can get insights such as win % by age and other nerdy metrics. To get familiar with the tables, you can see the existing tables on the Samples page.
  
  - Lastly, if you are excited about an upcoming fight, go to Tale of the Tape, and compare 2 fighters and their advanced metrics!
  """)
              
  st.caption('Please note that this a free, hosted application with data gathered by a 3rd party and not everything will be perfectly working at all times. However if you are a hardcore MMA fan, please use as you like. If you have questions or suggestions, a suggestion box will be introduced soon.') 

   if "inputs" not in st.session_state:
      st.session_state.inputs = []

  # Function to add text area input to the list
  def add_input():
      new_input = st.text_area("Enter your Suggestion")
      if new_input:
          st.session_state.inputs.append(new_input)
          st.success("Suggestion added to the list!")
  
  # Create a button to add text area input
  if st.button("Add Suggestion"):
      add_input()
  
  # Create a DataFrame from the list of inputs
  df = pd.DataFrame({"Suggestion": st.session_state.inputs})
  
  # Display the DataFrame
  st.write(df)
  
  st.header('Enjoy and JUST BLEED!')
  st.image('https://media.tenor.com/8jkYjD4cnqUAAAAM/just-bleed.gif')
  


elif view =='Fighter One Sheet':
    st.text('Display all relevant fighter stats in just 1 click. Choose your fighter below to get started')
    
    f1, f2  = st.columns(2)
    with f1:
        fighter_filter = st.selectbox('Pick a fighter',options=fighter_list)
    
    with f2:
        with st.container(border=True):
            analysis_lengths = ['Career','Last X fights']
            analysis_length = st.radio("Analysis Length",(analysis_lengths),horizontal=True)
            if analysis_length==analysis_lengths[1]:
                al = st.number_input('Number of recent fights to analyze',step=1,min_value=1)
                fr_cleaned = duckdb.sql(f"select * from fr_cleaned where FIGHTER1 = '{fighter_filter}' or FIGHTER2='{fighter_filter}' order by date desc limit {al}").df()
            
    
    st.divider()
    
    fights = duckdb.sql(f"SELECT BOUT from fr_cleaned where FIGHTER1 = '{fighter_filter}' or FIGHTER2='{fighter_filter}'").df()

    
    if len(fights)==0:
        st.write("No data for this fighter")
        st.stop()
      
    def calcFighterStats(fighter):
      winloss = duckdb.sql(f"SELECT case when FIGHTER1 = '{fighter}' then FIGHTER1_OUTCOME else FIGHTER2_OUTCOME end result from fr_cleaned where FIGHTER1 = '{fighter}' or FIGHTER2='{fighter}' ")
      last_fight= duckdb.sql(f"SELECT left(max(date)::string,10) max_date, left( (current_date() - max(date))::string,10) days_since from fr_cleaned where FIGHTER1= '{fighter}' or FIGHTER2='{fighter}' ").df()
      fighter_stats = duckdb.sql(f"SELECT * from fs_cleaned where BOUT in (select BOUT from fights) and FIGHTER ='{fighter}' ")
      cleaned_fighter_stats = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_str, sum(head_str_l::INTEGER) as head_str, sum(td_l::INTEGER) as td_l, round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2)  as td_rate, sum(kd::INTEGER) as kd, from fighter_stats").df()
      ko_wins = duckdb.sql(f"SELECT count(*) as s from fr_cleaned where ((FIGHTER1='{fighter}' and FIGHTER1_OUTCOME='W') OR (FIGHTER2='{fighter}' and FIGHTER2_OUTCOME='W')) and trim(METHOD)='KO/TKO' ").df()
      opp_stats = duckdb.sql(f"SELECT * from fs_cleaned where BOUT in (select * from fights) and FIGHTER !='{fighter}' ")
      cleaned_opp_stats = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_abs ,sum(head_str_l::INTEGER) as head_abs,sum(head_str_a::INTEGER) as head_at,sum(td_l::INTEGER) as td_abs,round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2) as td_abs_rate,sum(kd::INTEGER) as kd_abs from opp_stats").df()
      ko_losses = duckdb.sql(f"SELECT count(*) as s from fr_cleaned where ((FIGHTER1='{fighter}' and FIGHTER1_OUTCOME='L') OR (FIGHTER2='{fighter}' and FIGHTER2_OUTCOME='L')) and trim(METHOD)='KO/TKO' ").df()
      return winloss, last_fight, fighter_stats, cleaned_fighter_stats, ko_wins, opp_stats, cleaned_opp_stats, ko_losses
      
    fighterData = calcFighterStats(fighter_filter)
    winloss, last_fight, fighter_stats, cleaned_fighter_stats, ko_wins, opp_stats, cleaned_opp_stats, ko_losses = fighterData[0], fighterData[1], fighterData[2], fighterData[3], fighterData[4], fighterData[5], fighterData[6], fighterData[7]
    
    


    if fighter_filter:
        col1,col2,col3,col4,col5 = st.columns([0.3,0.5,0.3,0.5,0.6])
        with col1:
            st.subheader('Bio')
            st.divider()
            # ui.metric_card(title="Height", content=str(duckdb.sql(f"SELECT HEIGHT FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]), key="card1")
            st.metric(label='Height',value=str(duckdb.sql(f"SELECT HEIGHT FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]))
            # ui.metric_card('Division',content=str(duckdb.sql(f"SELECT WEIGHT FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]))
            st.metric(label='Division',value=str(duckdb.sql(f"SELECT WEIGHT FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]))
            # ui.metric_card('Reach',content=str(duckdb.sql(f"SELECT REACH FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]))
            st.metric(label='Reach', value=str(duckdb.sql(f"SELECT REACH FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]))
            dob_str = str(duckdb.sql(f"SELECT DOB FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0])
            dob = datetime.datetime.strptime(dob_str, '%b %d, %Y')
            age = datetime.datetime.now() - dob
            age_years = age.days // 365
            # ui.metric_card('Age',content=age_years, description=dob_str)
            st.metric(label='Age',value=age_years,delta=dob_str)
            if len(fights) >0:
                # ui.metric_card('Last fought', description=str(last_fight['max_date'].values[0]),content=str(last_fight['days_since'].values[0]))
                st.metric(label='Last Fought', value=str(last_fight['days_since'].values[0]), delta=str(last_fight['max_date'].values[0]))

        with col2:
            st.subheader('Highlights')
            st.divider()
            w1,w2 = st.columns(2)
            with w1:
                # ui.metric_card('UFC Fights',content=len(fights) )
                st.metric(label='UFC Fights',value=len(fights))
                # ui.metric_card('Rounds',content=fighter_stats.shape[0] )
                st.metric(label='Rounds',value=fighter_stats.shape[0])
            with w2:
                # ui.metric_card('Wins',content=len(duckdb.sql("SELECT * from winloss where result='W'").df()) )
                st.metric(label='Wins',value=len(duckdb.sql("SELECT * from winloss where result='W'").df()) )
                # ui.metric_card('Losses',content=len(duckdb.sql("SELECT * from winloss where result='L'").df()) )
                st.metric(label='Losses',value=len(duckdb.sql("SELECT * from winloss where result='L'").df()))
            
            # ui.metric_card('KO/TKO Wins', content = int(ko_wins['s'].iloc[0]) )
            st.metric(label='KO/TKO Wins',value=int(ko_wins['s'].iloc[0]))
            # ui.metric_card('KO/TKO Losses', content = int(ko_losses['s'].iloc[0]))
            st.metric(label='KO/TKO Losses',value=int(ko_losses['s'].iloc[0]))
        with col3:
            st.subheader('Striking')
            st.divider()
            # ui.metric_card('Significant Strikes Absored',content=int(cleaned_opp_stats['sig_abs'].iloc[0]))
            st.metric(label='Significant Strikes Absored',value=int(cleaned_opp_stats['sig_abs'].iloc[0]))
            # ui.metric_card('Head Strikes Absored',content=int(cleaned_opp_stats['head_abs'].iloc[0]))
            st.metric(label='Head Strikes Absored',value=int(cleaned_opp_stats['head_abs'].iloc[0]))
            # ui.metric_card('Significant Strikes Landed',content=int(cleaned_fighter_stats['sig_str'].iloc[0]))
            st.metric(label='Significant Strikes Landed',value=int(cleaned_fighter_stats['sig_str'].iloc[0]))
            # ui.metric_card('Head Strikes Landed',content=int(cleaned_fighter_stats['head_str'].iloc[0]))
            st.metric(label='Head Strikes Landed',value=int(cleaned_fighter_stats['head_str'].iloc[0]))
            # ui.metric_card('Knockdowns Landed',content=int(cleaned_fighter_stats['kd'].iloc[0]))
            st.metric(label='Knockdowns Landed',value=int(cleaned_fighter_stats['kd'].iloc[0]))
            # ui.metric_card('Knockdowns Absored',content=int(cleaned_opp_stats['kd_abs'].iloc[0]))
            st.metric(label='Knockdowns Absored',value=int(cleaned_opp_stats['kd_abs'].iloc[0]))
        with col4:
            st.subheader('Wrestling')
            st.divider()
            # ui.metric_card('Total Takedowns Landed',content=int(cleaned_fighter_stats['td_l'].iloc[0]),description="{0:.0%}".format(round(float(cleaned_fighter_stats['td_rate'].iloc[0]),2)))
            st.metric(label='Total Takedowns Landed',value=int(cleaned_fighter_stats['td_l'].iloc[0]),delta="{0:.0%}".format(round(float(cleaned_fighter_stats['td_rate'].iloc[0]),2)))
            # ui.metric_card('Total Takedowns Given Up',content=int(cleaned_opp_stats['td_abs'].iloc[0]),description="{0:.0%}".format(round(float(cleaned_opp_stats['td_abs_rate'].iloc[0]),2)))
            st.metric(label='Total Takedowns Given Up',value=int(cleaned_opp_stats['td_abs'].iloc[0]),delta="{0:.0%}".format(round(float(cleaned_opp_stats['td_abs_rate'].iloc[0]),2)))
        with col5:
            st.subheader('Adv. Stats')
            st.divider()
            st.metric('Significant Strikes Differential',value=round(cleaned_fighter_stats['sig_str']/cleaned_opp_stats['sig_abs'],1))
            st.metric('Head Strikes Differential',value=round(cleaned_fighter_stats['head_str']/cleaned_opp_stats['head_abs'],1))
            st.metric('Power Differential (Knockdowns)',value=round(cleaned_fighter_stats['kd']/cleaned_opp_stats['kd_abs'],1))
            st.metric('Takedown Differential',value=round(cleaned_fighter_stats['td_l']/cleaned_opp_stats['td_abs'],1))
            st.caption('Success rate at evading head strikes')
            head_movement = round(1-(cleaned_opp_stats['head_abs']/cleaned_opp_stats['head_at']),2)
            st.metric('Head Movement',value=head_movement )
        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            st.write("Strikes Attempted")
            
            str_a = duckdb.sql(f"SELECT DATE, sum(total_str_a::INT) as Total_Strikes_At from fighter_stats group by 1").df()
            fig = px.area(str_a, x='DATE', y='Total_Strikes_At', template='simple_white')
            st.plotly_chart(fig,use_container_width=True)
            # st.area_chart(str_a, x='DATE', y='Total_Strikes_At')
            st.write("Net Sig Strike Landed difference")
            str_dif = duckdb.sql(f"SELECT a.DATE, sum(a.sig_str_l::INT)-sum(b.sig_str_l::INT) as Strike_Diff from fighter_stats as a inner join opp_stats as b on a.DATE = b.DATE and a.BOUT=b.BOUT and a.ROUND=b.ROUND group by 1").df()
            fig = px.area(str_dif, x='DATE', y='Strike_Diff', template='simple_white')
            st.plotly_chart(fig,use_container_width=True)
            # st.area_chart(str_dif, x='DATE', y='Strike_Diff')
              
        with c2:
            st.write("Takedowns Attempted")
            td_a = duckdb.sql(f"SELECT DATE,  sum(td_a::int) TD_At from fighter_stats group by 1").df()
            fig = px.area(td_a, x='DATE', y='TD_At', template='simple_white')
            st.plotly_chart(fig,use_container_width=True)
            # st.area_chart(td_a, x='DATE', y='TD_At')
            st.write("Net Takedown difference")
            td_dif = duckdb.sql(f"SELECT a.DATE, sum(a.td_a::INT)-sum(b.td_a::INT) as TD_At_Diff from fighter_stats as a inner join opp_stats as b on a.DATE = b.DATE and a.BOUT=b.BOUT and a.ROUND=b.ROUND group by 1").df()
            fig = px.area(td_dif, x='DATE', y='TD_At_Diff', template='simple_white')
            st.plotly_chart(fig,use_container_width=True)
            # st.area_chart(td_dif, x='DATE', y='TD_At_Diff')

        st.divider()
        with st.expander("Career Results"):
            career_results = duckdb.sql(f"SELECT left(DATE::string,10) AS DATE ,EVENT,case when FIGHTER1='{fighter_filter}' then FIGHTER2 else FIGHTER1 end as OPPONENT,case when FIGHTER1='{fighter_filter}' then FIGHTER1_OUTCOME else FIGHTER2_OUTCOME end as RESULT,METHOD,ROUND, TIME,DETAILS from fr_cleaned where FIGHTER1= '{fighter_filter}' or FIGHTER2='{fighter_filter}' order by DATE desc").df()
            st.dataframe(career_results,hide_index=True)
    
    st.divider()
    with st.expander("Single Fight Stats"):
        bout_filter = st.selectbox('Pick a bout',options=fights.drop_duplicates())
        fight_results = duckdb.sql(f"SELECT * EXCLUDE (BOUT,FIGHTER,EVENT) from fs where replace(trim(BOUT),'  ',' ') ='{bout_filter}'  and trim(FIGHTER)='{fighter_filter}' ").df()
        
        if bout_filter:
             st.write(fight_results.set_index(fight_results.columns[0]).T)

elif view =='Show all dataset samples':
    st.write('Fighter Details (cleaned)')
    st.dataframe(duckdb.sql("select * from fighters limit 5").df(),hide_index=True)
    st.write('Events & Fights (cleaned)')
    st.dataframe(duckdb.sql("SELECT * from fed limit 5").df(),hide_index=True)
    st.write('Fight Results (cleaned)')
    st.dataframe(duckdb.sql("SELECT * from fr_cleaned limit 5").df(),hide_index=True)
    st.write('Fight Stats')
    st.dataframe(duckdb.sql("SELECT * from fs limit 5").df(),hide_index=True)
    st.write("Data Check - Events without data")
    anomalies = duckdb.sql("select left(DATE::string,10) as DATE,ed_c.EVENT, count(BOUT) as bouts_with_stats from ed_c left join fs on ed_c.EVENT =fs.EVENT group by 1,2 having bouts_with_stats=0 order by 1 desc").df()
    st.dataframe(anomalies,hide_index=True)
elif view =='Interesting Stats':
    st.subheader('Lifetime stats unless otherwise noted (last 2 years)')
    c1, c2  = st.columns(2)
    with c1:
        st.write("Fights by month")
        fights_monthly= duckdb.sql("SELECT date_trunc('month',date) as MONTH,count(*) as FIGHTS from fed group by 1 order by 1 asc").df()
        fig = px.area(fights_monthly, x='MONTH',y='FIGHTS', template='simple_white')
        st.plotly_chart(fig,use_container_width=True)
        # st.area_chart(fights_monthly, x='MONTH',y='FIGHTS')
        
        st.divider()

        st.write('Most experienced referees (2yr)')
        refs = duckdb.sql("SELECT REFEREE,count(*) fights from fr_cleaned where date between current_date() -730 and current_date() group by 1 order by 2 desc limit 10").df()
        st.dataframe(refs,hide_index=True,use_container_width=False)
        # ui.table(data=refs)
        st.divider()
        st.write("Fights by result method (2yr)")
        methods = duckdb.sql("SELECT method, count(*) FIGHTS from fr_cleaned where date between current_date() -730 and current_date() group by 1 ").df()
        fig = px.pie(methods,values='FIGHTS', names='METHOD', template='simple_white')
        st.plotly_chart(fig,use_container_width=True)
        # base = alt.Chart(methods).encode(alt.Theta("FIGHTS:Q").stack(True),alt.Color("METHOD:N").legend(None),alt.Tooltip("METHOD:N", title="METHOD"))
        # pie = base.mark_arc(outerRadius=120)
        # st.altair_chart(pie)
        
    
    with c2:
        st.write("Number of Fights per Fighter")
        fight_distro = duckdb.sql("""select FIGHTS, SUM(FIGHTERS) OVER (ORDER BY FIGHTS desc) FIGHTERS from
                                  (select FIGHTS,count(1) FIGHTERS from  (select FIGHTER,COUNT(DISTINCT EVENT||BOUT) FIGHTS from fs_cleaned group by 1) group by 1)
                              order by 1""").df()
        fig = px.bar(fight_distro, x='FIGHTS',y='FIGHTERS', template='simple_white')
        st.plotly_chart(fig,use_container_width=True)
        # st.bar_chart(fight_distro, x='FIGHTS',y='FIGHTERS')
        st.divider()
        
        st.write('Most commonly used venues (2yr)')
        locations = duckdb.sql("SELECT LOCATION,count(distinct EVENT) EVENTS from fed where date between current_date() -730 and current_date() group by 1 order by 2 desc limit 10").df()
        fig = px.bar(locations.sort_values(by='EVENTS'), x='EVENTS',y='LOCATION', template='simple_white')
        st.plotly_chart(fig,use_container_width=True)
       
        # base = alt.Chart(locations.sort_values(by='EVENTS')).mark_point().encode(x='EVENTS',y='LOCATION')
        # st.altair_chart(base)

        st.divider()
        st.write('Number of Fighters fought by Weight/Type (2yr)')
        fighters_by_class = duckdb.sql("""SELECT weightclass,count(distinct fighter) as fighters from 
            (SELECT replace(weightclass,' Bout','') as weightclass,FIGHTER1 fighter from fr_cleaned where date between current_date() -730 and current_date() group by 1,2 
            UNION 
            SELECT replace(weightclass,' Bout','') as weightclass,FIGHTER2 fighter from fr_cleaned where date between current_date() -730 and current_date() group by 1,2)
            group by 1
            """).df()
        st.dataframe(fighters_by_class,hide_index=True)

    st.write("Method of winning as a percentage of all methods over time")
    frame = st.selectbox('Pick a time dimension',['year','quarter','month','week','day'])
    methods_over_time = duckdb.sql(f"SELECT case when METHOD like 'Decision%' then 'Decision' else METHOD end as METHOD, date_trunc('{frame}',date) as MONTH, count(*)/sum(sum(1)) over (partition by MONTH) METHOD_PCT from fr_cleaned  group by 1,2 ").df()

    fig = px.area(methods_over_time, x='MONTH',y='METHOD_PCT',color='METHOD', template='simple_white')
    st.plotly_chart(fig,use_container_width=True)
    # st.area_chart(methods_over_time, x='MONTH',y='METHOD_PCT',color='METHOD')
elif view =='Aggregate Table':      
    min_fights = st.number_input('Minimum Fights',step=1,value=20)
    st.write(f"Minimum {min_fights} fights, historical rankings for total career offensive and defensive stats")
    
    fighters = duckdb.sql(f"SELECT fighter FROM fs_cleaned GROUP BY 1 having count(distinct BOUT||EVENT) >={min_fights} ").df()
    str_results = pd.DataFrame()
    all_time_offense = duckdb.sql(f"SELECT FIGHTER, COUNT(DISTINCT BOUT||EVENT) as FIGHTS, COUNT(*) AS ROUNDS,  ROUND(ROUNDS/CAST(FIGHTS as REAL),1) as ROUNDS_PER_FIGHT ,SUM(head_str_l::INTEGER) AS HEAD_STRIKES_LANDED, SUM(leg_str_l::INTEGER) as LEG_STRIKES_LANDED,sum(sig_str_l::INTEGER) as SIG_STRIKES_LANDED,sum(KD::INTEGER) as KD_LANDED, sum(TD_L::INT) as TD_LANDED from fs_cleaned group by 1 having FIGHTS>={min_fights}")
    
    def query_fighter_data(fighter):
      query = f"SELECT '{fighter}' AS FIGHTER, SUM(head_str_l::INTEGER) AS HEAD_STRIKES_ABS, SUM(head_str_a::INTEGER) AS HEAD_STRIKES_AT, SUM(sig_str_l::INTEGER) AS SIG_STRIKES_ABS, SUM(leg_str_l::INTEGER) as LEG_STRIKES_ABSORBED, sum(KD::INTEGER) as KD_ABSORED, sum(TD_L::INT) as TD_GIVEN_UP FROM fs_cleaned WHERE BOUT LIKE '%{fighter}%' AND fighter != '{fighter}'"
      return duckdb.sql(query).df()
    

    def oppStats():
      dfs_list = fighters['FIGHTER'].apply(query_fighter_data).tolist()
      str_results = pd.concat(dfs_list, ignore_index=True)
      return str_results
      
    with st.spinner('Calculating...'):
      str_results = oppStats()
      
    combined_stats = duckdb.sql("SELECT a.*, ROUND(SIG_STRIKES_LANDED/SIG_STRIKES_ABS,1) as SIG_STR_DIFF, ROUND((1-HEAD_STRIKES_ABS/HEAD_STRIKES_AT),2) as HEAD_MOVEMENT, b.* EXCLUDE (FIGHTER) from all_time_offense as a left join str_results as b on a.FIGHTER=b.FIGHTER").df()
    st.dataframe(combined_stats.sort_values(by='FIGHTS', ascending=False),hide_index=True)   
elif view=='SQL Editor':
    st.write("Write custom sql on the data using [🦆duckdb](https://duckdb.org/docs/archive/0.9.2/sql/introduction)")
    with st.expander("Examples"):
      st.write('Win % by age')
      st.code("""select age,  sum(W) as wins, sum(L) as losses, sum(fights) as total_results, sum(W)/(sum(W)+sum(L)) as win_pct from 
       (
      select date_diff('year',strptime(dob, '%b %d, %Y'),date)  as age, sum (case when fighter1_outcome = 'W' then 1 else 0 end) W, sum (case when fighter1_outcome = 'L' then 1 else 0 end) as L, count(1) fights from fighters inner join fr_cleaned on fighter = fighter1 where (weightclass ilike '%featherweight title%' )
      group by 1 
      UNION
      select date_diff('year',strptime(dob, '%b %d, %Y'),date)  as age, sum (case when fighter2_outcome = 'W' then 1 else 0 end) W, sum (case when fighter2_outcome = 'L' then 1 else 0 end) as L, count(1) fights from fighters inner join fr_cleaned on fighter = fighter2 where (weightclass ilike '%featherweight title%' )
      group by 1 
       )
      group by 1   
      """)
      st.write('Most significant strikes landed')
      st.code("""select event, bout, fighter, sum(sig_Str_l::int)  
              from fs_cleaned 
              group by 1,2,3 
              order by 4 desc  
              limit 20
              """)
      
    col1,col2 = st.columns([3,10])
    with col1:
        st.write('Tables')
        st.write('fs_cleaned = fight stats')
        st.write('fr_cleaned = fight results')
        st.write('fighters = fighter details')
    with col2:
        query_text = st_ace()
        # query_text = st.text_area('Write SELECT statement here')
        st.caption('Will add history of previous queries for reference')
        def pullData():
            query = duckdb.sql(f"{query_text.strip()}")
            return query
    
        if st.button('Pull data') and query_text is not None:
            try:
              with st.spinner('Running Query'):
                data = pullData()
                data = data.df()
                st.dataframe(data.head(100), hide_index=True)
                
            except Exception as e:
              st.write(e)
elif view=='Tale of the Tape':
  st.write('Compare advanced metrics between 2 fighters')
  c1, c2 = st.columns(2)

  fighter1_filter = c1.selectbox('Pick Fighter 1', options=fighter_list)
  fights1 = duckdb.sql(f"SELECT BOUT from fr_cleaned where FIGHTER1 = '{fighter1_filter}' or FIGHTER2='{fighter1_filter}'").df()
  fighter_stats1 = duckdb.sql(f"SELECT * from fs_cleaned where BOUT in (select BOUT from fights1) and FIGHTER ='{fighter1_filter}' ")
  cleaned_fighter_stats1 = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_str, sum(head_str_l::INTEGER) as head_str, sum(td_l::INTEGER) as td_l, round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2)  as td_rate, sum(kd::INTEGER) as kd, from fighter_stats1").df()
  opp_stats1 = duckdb.sql(f"SELECT * from fs_cleaned where BOUT in (select * from fights1) and FIGHTER !='{fighter1_filter}' ")
  cleaned_opp_stats1 = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_abs ,sum(head_str_l::INTEGER) as head_abs,sum(head_str_a::INTEGER) as head_at,sum(td_l::INTEGER) as td_abs,round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2) as td_abs_rate,sum(kd::INTEGER) as kd_abs from opp_stats1").df()
  
  c1.metric('Significant Strikes Differential', value=round(cleaned_fighter_stats1['sig_str']/cleaned_opp_stats1['sig_abs'],1))
  c1.metric('Head Strikes Differential', value=round(cleaned_fighter_stats1['head_str']/cleaned_opp_stats1['head_abs'],1))
  c1.metric('Power Differential (Knockdowns)', value=round(cleaned_fighter_stats1['kd']/cleaned_opp_stats1['kd_abs'],1))
  c1.metric(label='Total Takedowns Landed',value=int(cleaned_fighter_stats1['td_l'].iloc[0]),delta="{0:.0%}".format(round(float(cleaned_fighter_stats1['td_rate'].iloc[0]),2)))
  c1.metric(label='Total Takedowns Given Up',value=int(cleaned_opp_stats1['td_abs'].iloc[0]),delta="{0:.0%}".format(round(float(cleaned_opp_stats1['td_abs_rate'].iloc[0]),2)))
  c1.metric('Takedown Differential', value=round(cleaned_fighter_stats1['td_l']/cleaned_opp_stats1['td_abs'],1))
  c1.caption('Success rate at evading head strikes')
  head_movement1 = round(1-(cleaned_opp_stats1['head_abs']/cleaned_opp_stats1['head_at']),2)
  c1.metric('Head Movement', value=head_movement1)
  
  fighter2_filter = c2.selectbox('Pick Fighter 2', options=fighter_list)
  fights2 = duckdb.sql(f"SELECT BOUT from fr_cleaned where FIGHTER1 = '{fighter2_filter}' or FIGHTER2='{fighter2_filter}'").df()
  fighter_stats2 = duckdb.sql(f"SELECT * from fs_cleaned where BOUT in (select BOUT from fights2) and FIGHTER ='{fighter2_filter}' ")
  cleaned_fighter_stats2 = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_str, sum(head_str_l::INTEGER) as head_str, sum(td_l::INTEGER) as td_l, round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2)  as td_rate, sum(kd::INTEGER) as kd, from fighter_stats2").df()
  opp_stats2 = duckdb.sql(f"SELECT * from fs_cleaned where BOUT in (select * from fights2) and FIGHTER !='{fighter2_filter}' ")
  cleaned_opp_stats2 = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_abs ,sum(head_str_l::INTEGER) as head_abs,sum(head_str_a::INTEGER) as head_at,sum(td_l::INTEGER) as td_abs,round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2) as td_abs_rate,sum(kd::INTEGER) as kd_abs from opp_stats2").df()
  
  c2.metric('Significant Strikes Differential', value=round(cleaned_fighter_stats2['sig_str']/cleaned_opp_stats2['sig_abs'],1))
  c2.metric('Head Strikes Differential', value=round(cleaned_fighter_stats2['head_str']/cleaned_opp_stats2['head_abs'],1))
  c2.metric('Power Differential (Knockdowns)', value=round(cleaned_fighter_stats2['kd']/cleaned_opp_stats2['kd_abs'],1))
  c2.metric(label='Total Takedowns Landed',value=int(cleaned_fighter_stats2['td_l'].iloc[0]),delta="{0:.0%}".format(round(float(cleaned_fighter_stats2['td_rate'].iloc[0]),2)))
  c2.metric(label='Total Takedowns Given Up',value=int(cleaned_opp_stats2['td_abs'].iloc[0]),delta="{0:.0%}".format(round(float(cleaned_opp_stats2['td_abs_rate'].iloc[0]),2)))
  c2.metric('Takedown Differential', value=round(cleaned_fighter_stats2['td_l']/cleaned_opp_stats2['td_abs'],1))
  c2.caption('Success rate at evading head strikes')
  head_movement2 = round(1-(cleaned_opp_stats2['head_abs']/cleaned_opp_stats2['head_at']),2)
  c2.metric('Head Movement', value=head_movement2)
  
       
st.divider()
col1,col2,col3 = st.columns(3)
with col1:
  st.code('Built by Ilya')
with col2:
  st.code('This application uses data from Greco1899''s scraper of UFC Fight Stats - "https://github.com/Greco1899/scrape_ufc_stats"')
with col3:
  st.code('Recent changes - SQL Editor, data retrieval cached via function' )
# st.divider()
# with st.expander("Real UFC fans ONLY 🖱️",expanded=False):
#    audio_file = open('song.mp3', 'rb')
#    audio_bytes = audio_file.read()
#    st.audio(audio_bytes, format='audio/ogg')   

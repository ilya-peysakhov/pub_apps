import streamlit as st
import pandas as pd
import time
import polars as pl
import duckdb
import datetime
import plotly.express as px
import streamlit_shadcn_ui as ui



#additions
#strikes landed per minute over time with % over time
#strikes absored per fight

###################################


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

fr = fr.with_columns(
    pl.col("EVENT").str.replace("'",""),
    pl.col("BOUT").str.replace("'","")
)  

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

fs = fs.with_columns(
    pl.col("FIGHTER").str.replace("'",""),
    pl.col("BOUT").str.replace("'","")
)  

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


ft = ft.with_columns(
    pl.col("FIGHTER").str.replace("'","")
)  
fighters= duckdb.sql("SELECT trim(FIGHTER) as FIGHTER,HEIGHT,WEIGHT,REACH,STANCE,DOB,FIRST,LAST,NICKNAME,frd.URL from ft inner join frd on frd.URL = ft.URL")
########################
                      


#
if view =='Fighter One Sheet':
    st.text('Display all relevant fighter stats in just 1 click. Choose your fighter below to get started')
    fighter_list = duckdb.sql("SELECT FIGHTER from fighters  where length(DOB) >3 group by 1 order by 1").df()
    f1, f2  = st.columns(2)
    with f1:
        fighter_filter = st.selectbox('Pick a fighter',options=fighter_list)
    
    with f2:
        with st.container(border=True):
            analysis_lengths = ['Career','Last X fights']
            analysis_length = st.radio("Analysis Length",(analysis_lengths))
            if analysis_length==analysis_lengths[1]:
                al = st.number_input('Number of recent fights to analyze',step=1)
                fr_cleaned = duckdb.sql(f"select * from fr_cleaned where FIGHTER1 = '{fighter_filter}' or FIGHTER2='{fighter_filter}' order by date desc limit {al}").df()
            else:
                pass
    
    st.divider()
    
    fights = duckdb.sql(f"SELECT BOUT from fr_cleaned where FIGHTER1 = '{fighter_filter}' or FIGHTER2='{fighter_filter}'").df()
 
    
    if len(fights)==0:
        st.write("No data for this fighter")
        st.stop()

    
    
        
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
            st.divider()
            ui.metric_card(title="Height", content=str(duckdb.sql(f"SELECT HEIGHT FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]), key="card1")
            ui.metric_card('Division',content=str(duckdb.sql(f"SELECT WEIGHT FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]))
            ui.metric_card('Reach',content=str(duckdb.sql(f"SELECT REACH FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]))
                
            dob_str = str(duckdb.sql(f"SELECT DOB FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0])
            dob = datetime.datetime.strptime(dob_str, '%b %d, %Y')
            age = datetime.datetime.now() - dob
            age_years = age.days // 365
            ui.metric_card('Age',content=age_years, description=dob_str)
            if len(fights) >0:
                ui.metric_card('Last fought', description=str(last_fight['max_date'].values[0]),content=str(last_fight['days_since'].values[0]))


        with col2:
            st.subheader('Highlights')
            st.divider()
            w1,w2 = st.columns(2)
            with w1:
                ui.metric_card('UFC Fights',content=len(fights) )
                ui.metric_card('Rounds',content=fighter_stats.shape[0] )
            with w2:
                ui.metric_card('Wins',content=len(duckdb.sql("SELECT * from winloss where result='W'").df()) )
                ui.metric_card('Losses',content=len(duckdb.sql("SELECT * from winloss where result='L'").df()) )
            ui.metric_card('KO/TKO Wins', content = int(ko_wins['s'].iloc[0]) )
            ui.metric_card('KO/TKO Losses', content = int(ko_losses['s'].iloc[0]))
        with col3:
            st.subheader('Striking')
            st.divider()
            ui.metric_card('Significant Strikes Absored',content=int(cleaned_opp_stats['sig_abs'].iloc[0]))
            ui.metric_card('Head Strikes Absored',content=int(cleaned_opp_stats['head_abs'].iloc[0]))
            ui.metric_card('Significant Strikes Landed',content=int(cleaned_fighter_stats['sig_str'].iloc[0]))
            ui.metric_card('Head Strikes Landed',content=int(cleaned_fighter_stats['head_str'].iloc[0]))
            ui.metric_card('Knockdowns Landed',content=int(cleaned_fighter_stats['kd'].iloc[0]))
            ui.metric_card('Knockdowns Absored',content=int(cleaned_opp_stats['kd_abs'].iloc[0]))
        with col4:
            st.subheader('Wrestling')
            st.divider()
            ui.metric_card('Total Takedowns Landed',content=int(cleaned_fighter_stats['td_l'].iloc[0]),description="{0:.0%}".format(round(float(cleaned_fighter_stats['td_rate'].iloc[0]),2)))
            ui.metric_card('Total Takedowns Given Up',content=int(cleaned_opp_stats['td_abs'].iloc[0]),description="{0:.0%}".format(round(float(cleaned_opp_stats['td_abs_rate'].iloc[0]),2)))
                           
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
            st.write("Net Sig Strike Landed difference")
            str_dif = duckdb.sql(f"SELECT a.DATE, sum(a.sig_str_l::INT)-sum(b.sig_str_l::INT) as Strike_Diff from fighter_stats as a inner join opp_stats as b on a.DATE = b.DATE and a.BOUT=b.BOUT and a.ROUND=b.ROUND group by 1").df()
            fig = px.area(str_dif, x='DATE', y='Strike_Diff', template='simple_white')
            st.plotly_chart(fig,use_container_width=True)
              
        with c2:
            st.write("Takedowns Attempted")
            td_a = duckdb.sql(f"SELECT DATE,  sum(td_a::int) TD_At from fighter_stats group by 1").df()
            fig = px.area(td_a, x='DATE', y='TD_At', template='simple_white')
            st.plotly_chart(fig,use_container_width=True)
            st.write("Net Takedown difference")
            td_dif = duckdb.sql(f"SELECT a.DATE, sum(a.td_a::INT)-sum(b.td_a::INT) as TD_At_Diff from fighter_stats as a inner join opp_stats as b on a.DATE = b.DATE and a.BOUT=b.BOUT and a.ROUND=b.ROUND group by 1").df()
            fig = px.area(td_dif, x='DATE', y='TD_At_Diff', template='simple_white')
            st.plotly_chart(fig,use_container_width=True)


        st.divider()
        with st.expander("Career Results",expanded=False):
            career_results = duckdb.sql(f"SELECT left(DATE,10) AS DATE ,EVENT,case when FIGHTER1='{fighter_filter}' then FIGHTER2 else FIGHTER1 end as OPPONENT,case when FIGHTER1='{fighter_filter}' then FIGHTER1_OUTCOME else FIGHTER2_OUTCOME end as RESULT,METHOD,ROUND, TIME,DETAILS from fr_cleaned where FIGHTER1= '{fighter_filter}' or FIGHTER2='{fighter_filter}' order by DATE desc").df()
            st.dataframe(career_results,hide_index=True)
    
    st.divider()
    with st.expander("Single Fight Stats",expanded=False):
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
    anomalies = duckdb.sql("select left(DATE,10) as DATE,ed_c.EVENT, count(BOUT) as bouts_with_stats from ed_c left join fs on ed_c.EVENT =fs.EVENT group by 1,2 having bouts_with_stats=0 order by 1 desc").df()
    st.dataframe(anomalies,hide_index=True)
else:
    st.subheader('Lifetime stats unless otherwise noted (last 2 years)')
    c1, c2  = st.columns(2)
    with c1:
        st.write("Fights by month")
        fights_monthly= duckdb.sql("SELECT date_trunc('month',date) as MONTH,count(*) as FIGHTS from fed group by 1 order by 1 asc").df()
        fig = px.bar(fights_monthly, x='MONTH',y='FIGHTS', template='simple_white')
        st.plotly_chart(fig,use_container_width=True)
        
        st.divider()

        st.write('Most experienced referees (2yr)')
        refs = duckdb.sql("SELECT REFEREE,count(*) fights from fr_cleaned where date between current_date() -730 and current_date() group by 1 order by 2 desc limit 10").df()
        # st.dataframe(refs,hide_index=True,use_container_width=False)
        ui.table(data=refs)
        st.divider()
        st.write("Fights by result method (2yr)")
        methods = duckdb.sql("SELECT method, count(*) FIGHTS from fr_cleaned where date between current_date() -730 and current_date() group by 1 ").df()
        fig = px.pie(methods,values='FIGHTS', names='METHOD', template='simple_white')
        st.plotly_chart(fig,use_container_width=True)

        
    
    with c2:
        st.write("Number of Fights per Fighter")
        fight_distro = duckdb.sql("select FIGHTS,count(1) FIGHTERS from (select FIGHTER,COUNT(DISTINCT EVENT||BOUT) FIGHTS from fs_cleaned group by 1) group by 1 order by 1").df()
        fig = px.bar(fight_distro, x='FIGHTS',y='FIGHTERS', template='simple_white')
        st.plotly_chart(fig,use_container_width=True)
        
        st.divider()
        
        st.write('Most commonly used venues (2yr)')
        locations = duckdb.sql("SELECT LOCATION,count(distinct EVENT) EVENTS from fed where date between current_date() -730 and current_date() group by 1 order by 2 desc limit 10").df()
        fig = px.bar(locations.sort_values(by='EVENTS'), x='EVENTS',y='LOCATION', template='simple_white')
        st.plotly_chart(fig,use_container_width=True)

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
        
    min_fights = st.number_input('Minimum Fights',step=1,value=10)
    st.write(f"Minimum {min_fights} fights, historical rankings for total career offensive and defensive stats")
    fs_cleaned = fs_cleaned.pl()
    
    @st.cache_data(ttl='6d')
    def calcTotals(min_fights):
        fighters = duckdb.sql(f"SELECT fighter FROM fs_cleaned GROUP BY 1 having count(distinct BOUT||EVENT) >={min_fights} ").df()
        str_results = pd.DataFrame()

        def query_fighter_data(fighter):
            query = f"SELECT '{fighter}' AS FIGHTER, SUM(head_str_l::INTEGER) AS HEAD_STRIKES_ABS, SUM(head_str_a::INTEGER) AS HEAD_STRIKES_AT, SUM(sig_str_l::INTEGER) AS SIG_STRIKES_ABS, SUM(leg_str_l::INTEGER) as LEG_STRIKES_ABSORBED, sum(KD::INTEGER) as KD_ABSORED, sum(TD_L::INT) as TD_GIVEN_UP FROM fs_cleaned WHERE BOUT LIKE '%{fighter}%' AND fighter != '{fighter}'"
            return duckdb.sql(query).df()

        str_results = pd.concat(fighters['FIGHTER'].apply(query_fighter_data).tolist(), ignore_index=True)
        all_time_offense = duckdb.sql(f"SELECT FIGHTER, COUNT(DISTINCT BOUT||EVENT) as FIGHTS, COUNT(*) AS ROUNDS,  ROUND(ROUNDS/CAST(FIGHTS as REAL),1) as ROUNDS_PER_FIGHT ,SUM(head_str_l::INTEGER) AS HEAD_STRIKES_LANDED, SUM(leg_str_l::INTEGER) as LEG_STRIKES_LANDED,sum(sig_str_l::INTEGER) as SIG_STRIKES_LANDED,sum(KD::INTEGER) as KD_LANDED, sum(TD_L::INT) as TD_LANDED from fs_cleaned group by 1 having FIGHTS>={min_fights}")
        combined_stats = duckdb.sql("SELECT a.*, ROUND(SIG_STRIKES_LANDED/SIG_STRIKES_ABS,1) as SIG_STR_DIFF, ROUND((1-HEAD_STRIKES_ABS/HEAD_STRIKES_AT),2) as HEAD_MOVEMENT, b.* EXCLUDE (FIGHTER) from all_time_offense as a left join str_results as b on a.FIGHTER=b.FIGHTER").df()
        return combined_stats
        
    combined_stats = calcTotals(min_fights)
    st.dataframe(combined_stats.sort_values(by='FIGHTS', ascending=False),hide_index=True)   

       
       
    #with st.expander("Real UFC fans ONLY",expanded=False):
    #    audio_file = open('song.mp3', 'rb')
    #    audio_bytes = audio_file.read()
    #    st.audio(audio_bytes, format='audio/ogg')        

st.code('This application uses data from Greco1899''s scraper of UFC Fight Stats - "https://github.com/Greco1899/scrape_ufc_stats"')

st.code('Recent changes - new charts in All Time section, fixed stats for fighters with apostraphes,sped up all time rankings to 5s, add leg strikes, all data recovered by Greco1899' )

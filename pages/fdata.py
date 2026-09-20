import streamlit as st
import pandas as pd
import duckdb
import time
import datetime
from streamlit_ace import st_ace
import numpy as np

# Updated imports: native st.echarts (v1.64+) and externalized charts module
from utils.funcs import get_memory_usage, getData, cleanData, pullData, getFighters, query_fighter_data, oppStats, opp_stats,\
    fs, fed, fr_cleaned, fs_cleaned, fighters, ed_c
import charts as ch
##################################

def refreshData():
    getData.clear()
    cleanData.clear()
    st.rerun()
    st.toast("Data Refreshed!")

def calcFighterStats(fighter):
    winloss = duckdb.sql(f"SELECT case when FIGHTER1 = '{fighter}' then FIGHTER1_OUTCOME else FIGHTER2_OUTCOME end result from fr_cleaned where FIGHTER1 = '{fighter}' or FIGHTER2='{fighter}' ")
    last_fight = duckdb.sql(f"SELECT left(max(date)::string,10) max_date, left( (current_date() - max(date))::string,10) days_since from fr_cleaned where FIGHTER1= '{fighter}' or FIGHTER2='{fighter}' ").df()
    fighter_stats = duckdb.sql(f"SELECT * from fs_cleaned where BOUT in (select BOUT from fights) and FIGHTER ='{fighter}' ")
    cleaned_fighter_stats = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_str, sum(head_str_l::INTEGER) as head_str, sum(td_l::INTEGER) as td_l, round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2) as td_rate, sum(kd::INTEGER) as kd, from fighter_stats").df()
    ko_wins = duckdb.sql(f"SELECT count(*) as s from fr_cleaned where ((FIGHTER1='{fighter}' and FIGHTER1_OUTCOME='W') OR (FIGHTER2='{fighter}' and FIGHTER2_OUTCOME='W')) and trim(METHOD)='KO/TKO' ").df()
    opp_stats = duckdb.sql(f"SELECT * from fs_cleaned where BOUT in (select * from fights) and FIGHTER !='{fighter}' ")
    cleaned_opp_stats = duckdb.sql("SELECT sum(sig_str_l::INTEGER) as sig_abs ,sum(head_str_l::INTEGER) as head_abs,sum(head_str_a::INTEGER) as head_at,sum(td_l::INTEGER) as td_abs,round(sum(td_l::INTEGER)/cast(sum(td_a::REAL) as REAL),2) as td_abs_rate,sum(kd::INTEGER) as kd_abs from opp_stats").df()
    ko_losses = duckdb.sql(f"SELECT count(*) as s from fr_cleaned where ((FIGHTER1='{fighter}' and FIGHTER1_OUTCOME='L') OR (FIGHTER2='{fighter}' and FIGHTER2_OUTCOME='L')) and trim(METHOD)='KO/TKO' ").df()
    return winloss, last_fight, fighter_stats, cleaned_fighter_stats, ko_wins, opp_stats, cleaned_opp_stats, ko_losses

@st.cache_data(ttl='7d')
def get_fighter_list():
    return duckdb.sql("""SELECT distinct fighter1 as FIGHTER from fr_cleaned""").df()

########start of app

view = st.tabs(['Welcome','Fighter One Sheet','Interesting Stats','Aggregate Table','Show all dataset samples','SQL Editor','Tale of the Tape'],
               on_change='rerun',
               default='Welcome')

###################### data pull and clean
fed, fr_cleaned, fs_cleaned, fighters, ed_c = cleanData()

########################
fighter_list = get_fighter_list()                    

if view[0].open:
    with view[0]:
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
        
        if st.button('Refresh Data'):
            refreshData()
        st.header('Enjoy and JUST BLEED!')
        st.image('https://media.tenor.com/8jkYjD4cnqUAAAAM/just-bleed.gif')
        
elif view[1].open:
    with view[1]:
        st.text('Display all relevant fighter stats in just 1 click. Choose your fighter below to get started')
        
        flex = st.container(horizontal=True,horizontal_alignment='left')
        with flex.container(width=400):
            fighter_filter = st.selectbox('Pick a fighter',options=sorted(fighter_list['FIGHTER'].tolist()), width=400,index=None)
            if fighter_filter == None:
                st.stop()
                
        with flex.container():           
            analysis_lengths = ['Career','Last X fights']
            analysis_length = st.radio("Analysis Length",(analysis_lengths),horizontal=True)
            if analysis_length==analysis_lengths[1]:
                al = st.number_input('Number of recent fights to analyze',step=1,min_value=1)
                fr_cleaned = duckdb.sql(f"select * from fr_cleaned where FIGHTER1 = '{fighter_filter}' or FIGHTER2='{fighter_filter}' order by date desc limit {al}").df()
                
        st.divider()
        fights = duckdb.sql(f"SELECT BOUT from fr_cleaned where FIGHTER1 = '{fighter_filter}' or FIGHTER2='{fighter_filter}'").df() 
        if len(fights)==0:
            st.write("No data for this fighter")
        else:
            winloss, last_fight, fighter_stats, cleaned_fighter_stats, ko_wins, opp_stats, cleaned_opp_stats, ko_losses = calcFighterStats(fighter_filter)
        
        if fighter_filter:
            st.subheader('Bio')
            flex = st.container(horizontal=True,horizontal_alignment='left')
            flex.metric(label='Height',value=str(duckdb.sql(f"SELECT HEIGHT FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]),border=True,width='content')
            flex.metric(label='Division',value=str(duckdb.sql(f"SELECT WEIGHT FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]),border=True,width='content')
            flex.metric(label='Reach', value=str(duckdb.sql(f"SELECT REACH FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0]),border=True,width='content')
            
            dob_str = str(duckdb.sql(f"SELECT DOB FROM fighters WHERE FIGHTER = '{fighter_filter}'").df().iloc[0,0])
            dob = datetime.datetime.strptime(dob_str, '%b %d, %Y')
            age = datetime.datetime.now() - dob
            age_years = age.days // 365
            flex.metric(label='Age',value=age_years,delta=dob_str,border=True,width='content')
            if len(fights) >0:
                flex.metric(label='Last Fought', value=str(last_fight['days_since'].values[0]), delta=str(last_fight['max_date'].values[0]),border=True,width='content')
    
            col2,col3,col4,col5 = st.columns([0.5,0.5,0.5,0.6])
            with col2:
                st.subheader('Highlights')
                st.divider()
                w1,w2 = st.columns(2)
                with w1:
                    st.metric(label='UFC Fights',value=len(fights),border=True,width='content')
                    st.metric(label='Rounds',value=fighter_stats.shape[0],border=True,width='content')
                with w2:
                    st.metric(label='Wins',value=len(duckdb.sql("SELECT * from winloss where result='W'").df()) ,border=True,width='content')
                    st.metric(label='Losses',value=len(duckdb.sql("SELECT * from winloss where result='L'").df()),border=True,width='content')
                
                st.metric(label='KO/TKO Wins',value=int(ko_wins['s'].iloc[0]),border=True,width='content')
                st.metric(label='KO/TKO Losses',value=int(ko_losses['s'].iloc[0]),border=True,width='content')
                
            with col3:
                st.subheader('Striking')
                st.divider()
                st.metric(label='Significant Strikes Absored',value=int(cleaned_opp_stats['sig_abs'].iloc[0]),border=True,width='content')
                st.metric(label='Head Strikes Absored',value=int(cleaned_opp_stats['head_abs'].iloc[0]),border=True,width='content')
                st.metric(label='Significant Strikes Landed',value=int(cleaned_fighter_stats['sig_str'].iloc[0]),border=True,width='content')
                st.metric(label='Head Strikes Landed',value=int(cleaned_fighter_stats['head_str'].iloc[0]),border=True,width='content')
                st.metric(label='Knockdowns Landed',value=int(cleaned_fighter_stats['kd'].iloc[0]),border=True,width='content')
                st.metric(label='Knockdowns Absored',value=int(cleaned_opp_stats['kd_abs'].iloc[0]),border=True,width='content')
                
            with col4:
                st.subheader('Wrestling')
                st.divider()
                st.metric(label='Total Takedowns Landed',value=int(cleaned_fighter_stats['td_l'].iloc[0]),delta="{0:.0%}".format(round(float(cleaned_fighter_stats['td_rate'].iloc[0]),2)),border=True,width='content')
                st.metric(label='Total Takedowns Given Up',value=int(cleaned_opp_stats['td_abs'].iloc[0]),delta="{0:.0%}".format(round(float(cleaned_opp_stats['td_abs_rate'].iloc[0]),2)),border=True,width='content')
                
            with col5:
                st.subheader('Adv. Stats')
                st.divider()
                st.metric('Significant Strikes Differential',value=round(cleaned_fighter_stats['sig_str']/cleaned_opp_stats['sig_abs'],1),border=True,width='content')
                st.metric('Head Strikes Differential',value=round(cleaned_fighter_stats['head_str']/cleaned_opp_stats['head_abs'],1),border=True,width='content')
                st.metric('Power Differential (Knockdowns)',value=round(cleaned_fighter_stats['kd']/cleaned_opp_stats['kd_abs'],1),border=True,width='content')
                st.metric('Takedown Differential',value=round(cleaned_fighter_stats['td_l']/cleaned_opp_stats['td_abs'],1),border=True,width='content')
                st.caption('Success rate at evading head strikes')
                head_movement = round(1-(cleaned_opp_stats['head_abs']/cleaned_opp_stats['head_at']),2)
                st.metric('Head Movement',value=head_movement ,border=True,width='content')
            st.divider()
            
            c_str1, c_str2 = st.columns(2)
            with c_str1:
                str_a = duckdb.sql(f"SELECT DATE, sum(total_str_a::INT) as Total_Strikes_At from fighter_stats group by 1").df()
                st.echarts(options=ch.get_strikes_attempted_chart(str_a['DATE'].astype(str).tolist(), str_a['Total_Strikes_At'].tolist()), height="400px")
            
            with c_str2:
                str_dif = duckdb.sql(f"SELECT a.DATE, sum(a.sig_str_l::INT)-sum(b.sig_str_l::INT) as Strike_Diff from fighter_stats as a inner join opp_stats as b on a.DATE = b.DATE and a.BOUT=b.BOUT and a.ROUND=b.ROUND group by 1").df()
                st.echarts(options=ch.get_strike_diff_chart(str_dif['DATE'].astype(str).tolist(), str_dif['Strike_Diff'].tolist()), height="400px")
            
            c_td1, c_td2 = st.columns(2)
            with c_td1:
                td_a = duckdb.sql(f"SELECT DATE, sum(td_a::int) TD_At from fighter_stats group by 1").df()
                st.echarts(options=ch.get_td_attempted_chart(td_a['DATE'].astype(str).tolist(), td_a['TD_At'].tolist()), height="400px")
            
            with c_td2:
                td_dif = duckdb.sql(f"SELECT a.DATE, sum(a.td_a::INT)-sum(b.td_a::INT) as TD_At_Diff from fighter_stats as a inner join opp_stats as b on a.DATE = b.DATE and a.BOUT=b.BOUT and a.ROUND=b.ROUND group by 1").df()
                st.echarts(options=ch.get_td_diff_chart(td_dif['DATE'].astype(str).tolist(), td_dif['TD_At_Diff'].tolist()), height="400px")
    
            st.divider()
            cumulative_head_trauma = duckdb.sql(f"""
                SELECT 
                    date, 
                    SUM(SUM(head_str_l::int)) OVER (ORDER BY date ASC) AS head_str_l 
                FROM fs_cleaned 
                WHERE BOUT IN (SELECT * FROM fights) 
                  AND FIGHTER != '{fighter_filter}'  
                GROUP BY 1
            """).df()
            
            cumulative_head_trauma['DATE'] = pd.to_datetime(cumulative_head_trauma['DATE'])
            cumulative_head_trauma['fight_number'] = range(1, len(cumulative_head_trauma) + 1)
            
            slope, intercept = np.polyfit(cumulative_head_trauma['fight_number'], cumulative_head_trauma['head_str_l'], 1)
            cumulative_head_trauma['trend'] = slope * cumulative_head_trauma['fight_number'] + intercept
            
            dates_str = cumulative_head_trauma['DATE'].dt.strftime('%Y-%m-%d').tolist()
            
            st.echarts(
                options=ch.get_cum_trauma_chart(
                    dates_str, 
                    cumulative_head_trauma['head_str_l'].tolist(), 
                    cumulative_head_trauma['trend'].tolist(), 
                    slope
                ), 
                height="500px"
            )
            
            st.divider()
            with st.expander("Career Results"):
                try:
                    career_results = duckdb.sql(f"SELECT left(DATE::string,10) AS DATE ,EVENT,case when FIGHTER1='{fighter_filter}' then FIGHTER2 else FIGHTER1 end as OPPONENT,case when FIGHTER1='{fighter_filter}' then FIGHTER1_OUTCOME else FIGHTER2_OUTCOME end as RESULT,METHOD,ROUND, TIME,DETAILS from fr_cleaned where FIGHTER1= '{fighter_filter}' or FIGHTER2='{fighter_filter}' order by DATE desc").df()
                    st.dataframe(career_results,hide_index=True)
                except Exception as e:
                    st.caption('There may be duplicate fights in the data which are causing an issue since they are labeled the same')
                    st.error(e)

    st.divider()
    with st.expander("Single Fight Stats"):
        try:
            bout_filter = st.selectbox('Pick a bout',options=fights.drop_duplicates())
            fight_results = duckdb.sql(f"SELECT * EXCLUDE (BOUT,FIGHTER,EVENT) from fs where replace(trim(BOUT),'  ',' ') ='{bout_filter}'  and trim(FIGHTER)='{fighter_filter}' ").df()
            
            if bout_filter:
                 st.write(fight_results.set_index(fight_results.columns[0]).T)
        except Exception as e:
            st.caption('There may be duplicate fights in the data which are causing an issue since they are labeled the same')
            st.error(e)
        
elif view[2].open:
    with view[2]:
        st.subheader('Lifetime stats unless otherwise noted (last 2 years)')
        c1, c2 = st.columns(2)
        with c1:
            st.write("Fights by month")
            fights_monthly= duckdb.sql("SELECT date_trunc('month',date) as MONTH,count(*) as FIGHTS, count(distinct EVENT) as EVENTS from fed group by 1 order by 1 asc").df()
            
            st.echarts(
                options=ch.get_monthly_fights_chart(
                    fights_monthly['MONTH'].astype(str).tolist(),
                    fights_monthly['FIGHTS'].tolist(),
                    fights_monthly['EVENTS'].tolist()
                ), 
                height="400px"
            )
            
            st.divider()
    
            st.write('Most experienced referees (2yr)')
            refs = duckdb.sql("SELECT REFEREE,count(*) fights from fr_cleaned where date between current_date() -730 and current_date() group by 1 order by 2 desc limit 10").df()
            st.dataframe(refs,hide_index=True,width='content')
            st.divider()
            st.write("Fights by result method (2yr)")
            methods = duckdb.sql("SELECT method, count(*) FIGHTS from fr_cleaned where date between current_date() -730 and current_date() group by 1 ").df()
            
            pie_data = [{"value": row['FIGHTS'], "name": row['METHOD']} for _, row in methods.iterrows()]
            st.echarts(options=ch.get_methods_pie_chart(pie_data), height="400px")
            
        with c2:
            st.write("Number of Fights per Fighter")
            fight_distro = duckdb.sql("""select FIGHTS, SUM(FIGHTERS) OVER (ORDER BY FIGHTS desc) FIGHTERS from
                                      (select FIGHTS,count(1) FIGHTERS from  (select FIGHTER,COUNT(DISTINCT EVENT||BOUT) FIGHTS from fs_cleaned group by 1) group by 1)
                                  order by 1""").df()
            
            st.echarts(
                options=ch.get_fight_distro_chart(
                    fight_distro['FIGHTS'].astype(str).tolist(),
                    fight_distro['FIGHTERS'].tolist()
                ), 
                height="400px"
            )
            st.divider()
            
            st.write('Most commonly used venues (2yr)')
            locations = duckdb.sql("SELECT LOCATION,count(distinct EVENT) EVENTS from fed where date between current_date() -730 and current_date() group by 1 order by 2 desc limit 10").df()
            loc_sorted = locations.sort_values(by='EVENTS')
            
            st.echarts(
                options=ch.get_locations_chart(
                    loc_sorted['LOCATION'].tolist(),
                    loc_sorted['EVENTS'].tolist()
                ), 
                height="400px"
            )
    
            st.divider()
            st.write('Number of Fighters fought by Weight/Type (2yr)')
            fighters_by_class = duckdb.sql("""SELECT weightclass,count(distinct fighter) as fighters from 
                (SELECT replace(weightclass,' Bout','') as weightclass,FIGHTER1 fighter from fr_cleaned where date between current_date() -730 and current_date() group by 1,2 
                UNION 
                SELECT replace(weightclass,' Bout','') as weightclass,FIGHTER2 fighter from fr_cleaned where date between current_date() -730 and current_date() group by 1,2)
                group by 1
                """).df()
            st.dataframe(fighters_by_class,hide_index=True)
    
        st.divider()
        st.write("Method of winning as a percentage of all methods over time")
        frame = st.selectbox('Pick a time dimension',['year','quarter','month','week','day'])
        fr_cleaned_duck = fr_cleaned.copy()

        frame_map = {
            'month': 'M',
            'quarter': 'Q', 
            'year': 'Y',
            'week': 'W',
            'day': 'D'
        }
        freq = frame_map.get(frame, frame)
        methods_over_time = (
        fr_cleaned_duck
            .assign(METHOD=lambda x: x['METHOD'].str.replace(r'^Decision.*', 'Decision', regex=True),
                    MONTH=lambda x: x['DATE'].dt.to_period(freq).dt.to_timestamp())
            .groupby(['METHOD', 'MONTH'])
            .size()
            .reset_index(name='cnt')
            .assign(METHOD_PCT=lambda x: x['cnt'] / x.groupby('MONTH')['cnt'].transform('sum'))
            .drop(columns='cnt')
        )
        
        unique_months = pd.DatetimeIndex(sorted(methods_over_time['MONTH'].unique()))
        months_str = [m.strftime('%Y-%m-%d') for m in unique_months]
        unique_methods = methods_over_time['METHOD'].unique().tolist()
        
        series_list = []
        for m_name in unique_methods:
            m_df = (
                methods_over_time[methods_over_time['METHOD'] == m_name]
                .drop_duplicates(subset=['MONTH'])
                .set_index('MONTH')
            )
            
            pct_series = m_df['METHOD_PCT'].reindex(unique_months, fill_value=0.0)
            
            series_list.append({
                "name": str(m_name),
                "type": "line",
                "stack": "Total",
                "areaStyle": {},
                "data": pct_series.tolist()
            })

        st.echarts(
            options=ch.get_methods_over_time_chart(unique_methods, months_str, series_list), 
            height="500px"
        )

elif view[3].open:
    with view[3]:
        min_fights = st.number_input('Minimum Fights',step=1,value=10,width=100)
        st.write(f"Minimum {min_fights} fights, historical rankings for total career offensive and defensive stats")
            
        with st.spinner('Gathering Offense...'):
            all_time_offense = duckdb.sql(f"SELECT FIGHTER, COUNT(DISTINCT BOUT||EVENT) as FIGHTS, COUNT(*) AS ROUNDS,  ROUND(ROUNDS/CAST(FIGHTS as REAL),1) as ROUNDS_PER_FIGHT ,SUM(head_str_l::INTEGER) AS HEAD_STRIKES_LANDED, SUM(leg_str_l::INTEGER) as LEG_STRIKES_LANDED,sum(sig_str_l::INTEGER) as SIG_STRIKES_LANDED,sum(KD::INTEGER) as KD_LANDED, sum(TD_L::INT) as TD_LANDED from fs_cleaned group by 1 having FIGHTS>={min_fights}")
          
        with st.spinner('Gathering Defense...'):
            str_results= opp_stats()
        
        with st.spinner('Combining all data...'):
            combined_stats = duckdb.sql("SELECT a.*, ROUND(SIG_STRIKES_LANDED/SIG_STRIKES_ABS,1) as SIG_STR_DIFF, ROUND((1-HEAD_STRIKES_ABS/HEAD_STRIKES_AT),2) as HEAD_MOVEMENT, ROUND(HEAD_STRIKES_ABS/ROUNDS,1) as HEAD_STRIKES_ABS_PER_ROUND, b.* EXCLUDE (FIGHTER) from all_time_offense as a left join str_results as b on a.FIGHTER=b.FIGHTER").df()
        
        st.dataframe(combined_stats.sort_values(by='FIGHTS', ascending=False),hide_index=True) 
    
        @st.fragment
        def vizPlot():
            c1, c2 = st.columns(2)
            chart_metric1 = c1.selectbox('Choose a metric to plot',combined_stats.columns)
            chart_metric2 = c2.selectbox('Choose a second metric to plot',combined_stats.columns)
            viz_data = duckdb.sql(f"select FIGHTER, {chart_metric1}, {chart_metric2} from combined_stats ").df()
            x = viz_data[chart_metric1]
            y = viz_data[chart_metric2]
            slope, intercept = np.polyfit(x, y, 1)
            
            scatter_series_data = [
                [row[chart_metric1], row[chart_metric2], row['FIGHTER']]
                for _, row in viz_data.iterrows()
            ]
            
            x_min, x_max = float(x.min()), float(x.max())
            trendline_data = [
                [x_min, slope * x_min + intercept],
                [x_max, slope * x_max + intercept]
            ]
            
            st.echarts(
                options=ch.get_scatter_chart(
                    chart_metric1, 
                    chart_metric2, 
                    scatter_series_data, 
                    trendline_data, 
                    slope
                ), 
                height="550px"
            )
        
        # vizPlot()

elif view[4].open:
    with view[4]:
        st.write('Fighter Details (cleaned)')
        st.dataframe(duckdb.sql("select * from fighters limit 5").df(),hide_index=True, width='content')
        st.write('Events & Fights (cleaned)')
        st.dataframe(duckdb.sql("SELECT * from fed limit 5").df(),hide_index=True, width='content')
        st.write('Fight Results (cleaned)')
        st.dataframe(duckdb.sql("SELECT * from fr_cleaned limit 5").df(),hide_index=True, width='content')
        st.write('Fight Stats')
        st.dataframe(duckdb.sql("SELECT * from fs limit 5").df(),hide_index=True, width='content')
        st.write("Data Check - Events without data")
        anomalies = duckdb.sql("select left(DATE::string,10) as DATE,ed_c.EVENT, count(BOUT) as bouts_with_stats from ed_c left join fs on ed_c.EVENT =fs.EVENT group by 1,2 having bouts_with_stats=0 order by 1 desc").df()
        st.dataframe(anomalies,hide_index=True, width='content')
        
elif view[5].open:
    with view[5]:
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
            st.write('Womens bouts with the most combined strikes')
            st.code("""select event, bout, sum(sig_str_l) as total_sig_strikes, avg(rounds) as rounds, 
               round(sum(sig_str_l)/avg(rounds)) as sig_per_round
              from 
              (
              select event, bout, fighter,sum(sig_str_l::int)

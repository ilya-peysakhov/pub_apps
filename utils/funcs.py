import psutil
import streamlit as st
import duckdb
import pandas as pd
import polars as pl 

def get_memory_usage():
  memory_usage = psutil.virtual_memory().used
  total_memory = psutil.virtual_memory().total
  memory_usage_percentage = (memory_usage / total_memory) * 100
  return memory_usage_percentage
  
@st.cache_data(ttl = '7d')
def getData():
  ed = pl.read_csv("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_event_details.csv").df()
  # fd_greco = duckdb.read_csv("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fight_details.csv").df()
  # fd_backfill = duckdb.read_csv("https://raw.githubusercontent.com/ilya-peysakhov/grecoscraper/main/ufcdata/details.csv").df()
  # fd = duckdb.sql("select * from fd_greco UNION select* from fd_backfill").df()
  fd = pl.read_csv("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fight_details.csv").df()
  
  # fr_greco = duckdb.read_csv("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fight_results.csv").df()
  # fr_backfill = duckdb.read_csv("https://raw.githubusercontent.com/ilya-peysakhov/grecoscraper/main/ufcdata/results.csv").df()
  # fr = duckdb.sql("select * from fr_greco UNION select* from fr_backfill").df()
  fr =  pl.read_csv("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fight_results.csv").df()
  
  # fs_greco = duckdb.read_csv("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fight_stats.csv").df()
  # fs_greco = pd.read_csv("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fight_stats.csv",engine='pyarrow',dtype_backend='pyarrow')
  # fs_backfill = duckdb.read_csv("https://raw.githubusercontent.com/ilya-peysakhov/grecoscraper/main/ufcdata/stats.csv").df()
  # fs = duckdb.sql("select * from fs_greco UNION select * from fs_backfill").df()
  fs = pl.read_csv("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fight_stats.csv").to_pandas()
  # fs[['HEAD','BODY','LEG','DISTANCE','CLINCH','GROUND']] = fs[['HEAD','BODY','LEG','DISTANCE','CLINCH','GROUND']].astype(str)
  
  frd = pl.read_csv("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fighter_details.csv").df()
  ft = pl.read_csv("https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fighter_tott.csv").df()
  return ed, fd, fr, fs, frd, ft

ed, fd, fr, fs, frd, ft = getData()

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
  
fed, fr_cleaned, fs_cleaned, fighters, ed_c = cleanData()

def pullData(query_text):
  query = duckdb.sql(f"{query_text.strip()}")
  return query

def getFighters(min_fights):
  fighters = duckdb.sql(f"SELECT fighter FROM fs_cleaned GROUP BY 1 having count(distinct BOUT||EVENT) >={min_fights}").df()
  return fighters
        
def query_fighter_data(fighter):
  query = f"SELECT '{fighter}' AS FIGHTER, SUM(head_str_l::INTEGER) AS HEAD_STRIKES_ABS, SUM(head_str_a::INTEGER) AS HEAD_STRIKES_AT, SUM(sig_str_l::INTEGER) AS SIG_STRIKES_ABS, SUM(leg_str_l::INTEGER) as LEG_STRIKES_ABSORBED, sum(KD::INTEGER) as KD_ABSORED, sum(TD_L::INT) as TD_GIVEN_UP FROM fs_cleaned WHERE BOUT LIKE '%{fighter}%' AND fighter != '{fighter}'"
  return duckdb.sql(query).df()
    
def oppStats():
  dfs_list = fighters['FIGHTER'].apply(query_fighter_data).tolist()
  str_results = pd.concat(dfs_list, ignore_index=True)
  return str_results

def opp_stats():
     return duckdb.sql("""SELECT a.FIGHTER , SUM(head_str_l::INTEGER) AS HEAD_STRIKES_ABS, SUM(head_str_a::INTEGER) AS HEAD_STRIKES_AT, SUM(sig_str_l::INTEGER) AS SIG_STRIKES_ABS, SUM(leg_str_l::INTEGER) as LEG_STRIKES_ABSORBED, sum(KD::INTEGER) as KD_ABSORED, sum(TD_L::INT) as TD_GIVEN_UP 
     FROM  (select  fighter from fs_cleaned group by 1) a 
     inner join fs_cleaned b on a.fighter!=b.fighter and b.BOUT ilike '%' || a.fighter || '%'
     group by 1""").df()

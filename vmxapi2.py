import streamlit as st
import requests
import xml.etree.ElementTree as ET
import time
import pandas as pd


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


st.set_page_config(page_icon="✂️", page_title="VMX API")

#FUNCTIONS
#submits the report with preset sites, using the provided time and target
##new
def submitReports(targets, time_periods):
    for target in targets:
        for time_period in time_periods:
            submitted_job_ids = []
            payload = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\n  <soap:Body>\n    <SubmitReport xmlns=\"http://comscore.com/\">\n      <query xmlns=\"http://comscore.com/ReportQuery\">\n        <Parameter KeyId=\"geo\" Value=\"840\" />\n        <Parameter KeyId=\"timeType\" Value=\"1\" />\n\t\t\t\t<Parameter KeyId=\"targetType\" Value=\"0\" />\n\t\t\t\t<Parameter KeyId=\"videoType\" Value=\"80002\" />\n\t\t\t\t<Parameter KeyId=\"dataSource\" Value=\"28\" />\n\t\t\t\t<Parameter KeyId=\"target\" Value=\""+str(target)+"\" />\t\t\t\n\t\t\t\t<Parameter KeyId=\"timePeriod\" Value=\""+str(time_period)+"\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33415228\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33421956\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33420871\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36116392\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33420697\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33420049\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33420085\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33415239\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"5370508\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33417750\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33396602\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33396632\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33415247\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"5370530\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33417758\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33417466\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"28226546\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"28226614\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36907249\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36907451\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"28226139\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"5370519\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33415257\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"28226295\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36116408\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36907127\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"5370511\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"5370512\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"28227756\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"28227760\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"28227454\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33420054\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"5370509\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33421080\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36116396\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36116424\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33421126\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"28226461\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33417768\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36116400\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"28227310\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36116448\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33420739\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"28226622\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36116485\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"33420340\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"28226551\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36116416\" />\n\t\t\t\t<Parameter KeyId=\"media\" Value=\"36116456\" />\n        <PMInput PlatformId=\"130\" MeasureId=\"424\" />\n        <PMInput PlatformId=\"130\" MeasureId=\"489\" />\n\t\t\t\t<PMInput PlatformId=\"130\" MeasureId=\"490\" />\n\t\t\t\t<PMInput PlatformId=\"130\" MeasureId=\"159\" />\n\t\t\t\t<PMInput PlatformId=\"130\" MeasureId=\"161\" />\n\t\t\t\t<PMInput PlatformId=\"130\" MeasureId=\"162\" />\n\t\t\t\t<PMInput PlatformId=\"130\" MeasureId=\"163\" />\n\t\t\t\t<PMInput PlatformId=\"130\" MeasureId=\"2\" />\n      </query>\n    </SubmitReport>\n  </soap:Body>\n</soap:Envelope>"
            headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "Host": "api.comscore.com",
            "SOAPAction": "http://comscore.com/SubmitReport"
            }
            response = requests.request("POST", url, data=payload, headers=headers, auth=auth)
            root = ET.fromstring(response.text)
            #time.sleep(0.5)
            ns = {'default': 'http://comscore.com/Report'}
            submitted_job_id = root.find('.//default:JobId', ns).text
            submitted_job_ids.append(submitted_job_id)
    return submitted_job_ids


#checks the jobID generated by the above
def checkJobs(job_ids):
    job_statuses = {}
    for job_id in job_ids:
        payload = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\n  <soap:Body>\n    <PingReportStatus xmlns=\"http://comscore.com/\">\n      <jobId>" + str(job_id) + "</jobId>\n    </PingReportStatus>\n  </soap:Body>\n</soap:Envelope>"
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "Host": "api.comscore.com",
            "SOAPAction": "http://comscore.com/PingReportStatus"
        }
        auth = requests.auth.HTTPBasicAuth(username, password)
        response2 = requests.request("POST", url, data=payload, headers=headers, auth=auth)
        root = ET.fromstring(response2.text)
        ns = {'default': 'http://comscore.com/Report'}
        job_status = root.find('.//default:Status', ns).text
        job_statuses[job_id] = job_status

    all_completed = False
    while not all_completed:
        all_completed = True
        for job_id, job_status in job_statuses.items():
            if job_status != 'Completed':
                all_completed = False
                break
        if not all_completed:
            time.sleep(3)
            job_statuses = checkJobs(job_ids) # re-check the status of all jobs
        
        
    return job_statuses


#fetches the jobID
def fetch(job_ids):
    responses = []
    for job_id in job_ids:
        payload = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\n  <soap:Body>\n    <FetchReport xmlns=\"http://comscore.com/\">\n      <jobId>"+str(job_id)+"</jobId>\n    </FetchReport>\n  </soap:Body>\n</soap:Envelope>"
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "Host": "api.comscore.com",
            "SOAPAction": "http://comscore.com/FetchReport"
        }    
        response = requests.request("POST", url, data=payload, headers=headers, auth=auth)
        responses.append(response)
    return responses


#
# st.text("Use this to pull VMX reports for a specific time period and demo. The sites are prefixed")
#SECTIONS

#AUTH

# st.image("https://miro.medium.com/max/1400/1*IH10jlQEJ7GW1_oq8s7WPw.png",width=200)
# st.text("Set the VMX Filters")

#VMX filters as columns
creds, tp,t = st.columns(3)

with creds:
    st.caption("Use your VMX credentials")
    username = st.text_input(label='Username')
    password = st.text_input(label='PW', type='password')


with tp:
    time_periods = st.multiselect('Months',('265','266','267','268','269','270','271','272','273','274','275','276')) 
    subc1,subc2 = st.columns(2)
    with subc1:
        gender = st.radio("Demo",('Persons','Females','Males','All'))
    with subc2:
        st.write('Toggle between Persons/Females/Males')

with t:
    if gender=='Persons':
        targets = st.multiselect('Targets',('1753','1750','1419','26','1555','1748','1746','6','1417','30','1853','1856'))
    elif gender=='Females':
        targets = st.multiselect('Targets',('1602','1765','1420','94','1538','1763','1761','74','1415','98','1855','1858'))
    elif gender=='Males':
        targets = st.multiselect('Targets',('1601','1758','1418','60','1520','1756','1754','40','1416','64','1854','1857'))
    elif gender=='All':
        targets = st.multiselect('Targets',('1753','1750','1419','26','1555','1748','1746','6','1417','30','1853','1856','1602','1765','1420','94','1538','1763','1761','74','1415','98','1855','1858','1601','1758','1418','60','1520','1756','1754','40','1416','64','1854','1857'))

#API Details
url = "https://api.comscore.com/VideoMetrix/VideoMetrixKeyMeasures2.asmx"
auth = requests.auth.HTTPBasicAuth(username, password)


t_df = pd.DataFrame(targets, columns=["target"])
tp_df = pd.DataFrame(time_periods, columns = ["time_period"])
subs_df = t_df.merge(tp_df,how='cross')
st.write(str(subs_df.shape[0])+' Jobs to Submit')

#SUBMIT AND DOWNLOAD 
# st.image("https://miro.medium.com/max/1400/1*IH10jlQEJ7GW1_oq8s7WPw.png")
# st.text("Submit and download your report")

#st function for CSV downloading
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')


#submit button will do everything up to download
if st.button("Submit Report") and time_periods is not None and targets is not None:
    st.image("https://i.pinimg.com/originals/57/a3/4a/57a34a1940e99950e5f21f62e5b8dff7.gif",width=400)
    job_ids = []
    for target in targets:
        for time_period in time_periods:
            job_id = submitReports([target], [time_period])[0] # submit report for this combination of target and time period
            #st.write('✅ Job '+str(job_id)+' has been submitted for target '+str(target))
            job_ids.append(job_id)
    jobStatus = checkJobs(job_ids)
    response3=fetch(job_ids)
    st.write("Reports are being fetched")
        #CSV section. parse through the response and convert XML to dataframe.
    combined_df = pd.DataFrame()
    for response in response3:
        root = ET.fromstring(response.text)
        # # Define the namespace used in the XML file
        ns = {'default': 'http://comscore.com/Report'}
        #pull Month from XML then inject into DF
        clean_month = root.find('.//default:TIMEPERIOD', ns).text
        target_column = root.find('.//default:TARGET', ns).text
        # # Extract the data from the XML file
        data = []
        for row in root.findall('.//default:TR', ns):
            cells = row.findall('.//default:TH', ns) + row.findall('.//default:TD', ns)
            data.append([cell.text for cell in cells])
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])
        #drop the very first row then reset the index
        df = df.drop([0]) 
        df = df.reset_index(drop=True)
        #set new top row as column header
        df.columns = df.iloc[0]
        #drop top row
        df = df.drop([0]) 
        df = df.assign(target=target_column)
        df = df.assign(month=clean_month)
        combined_df = pd.concat([combined_df,df], axis=0)
    st.write(combined_df.groupby(['target','month']).size())
    st.write(job_ids)
    csv = convert_df(combined_df)
    st.download_button(label="✅ Download CSV",data=csv,file_name='newfile.csv')
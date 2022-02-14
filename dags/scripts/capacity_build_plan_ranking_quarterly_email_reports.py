import requests
import pandas as pd
from datetime import date, timedelta, datetime
import csv
import os
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import urllib3
urllib3.disable_warnings()

def splunk_to_df(query):
    data = {
      'search': query,
      'output_mode': 'csv'
    }

    response = requests.post('https://splunkpba.osc.tac.net:8089/servicesNS/scheduler_capacity/telus_whsia/search/jobs/export/', data=data, verify=False,
                         auth=('scheduler_capacity', 'S3rv1c3Acc0unt2017!'))
                         
    decoded_content = response.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')

    #put output into dataframe
    my_list = list(cr)
    dfObj = pd.DataFrame(my_list)

    new_header = dfObj.iloc[0] #grab the first row for the header
    dfObj = dfObj[1:] #take the data less the header row
    dfObj.columns = new_header #set the header row as the df header

    return dfObj

def main():
    query_current_year_activity_level = """| loadjob savedsearch="scheduler_capacity:telus_whsia:Build Plan Ranking"
    | where Maestro_Site_On_Air_Completed = 0 AND Maestro_Plan_Id = tonumber(strftime(now(),"%Y")) 
    | table Province Engineering_Sub_Market PM SiteID WorkType BuildPlanPriority TotalScore Primary_Drivers ImplementationRequiredDate  Maestro_Site_On_Air Maestro_Site_On_Air_Completed Maestro_Plan_Id Maestro_Cell_Type Maestro_InvestmentDriver TriggeredSiteID Next_Special_Event Next_Special_Event_Date First_Special_Event First_Special_Event_Date
    | rename First_Special_Event as First_Special_Event_In_Plan_Year, First_Special_Event_Date as First_Special_Event_Date_In_Plan_Year
    | replace  "AB" with "Alberta", "BC" with "British Columbia", "PQ" with "Quebec", "Ontario Eastern" with "Ontario", "ON" with "Ontario" in Province
    | sort -TotalScore"""

    query_next_year_activity_level = """| loadjob savedsearch="scheduler_capacity:telus_whsia:Build Plan Ranking"
    | where Maestro_Site_On_Air_Completed = 0 AND Maestro_Plan_Id = tonumber(strftime(now(),"%Y")) + 1
    | table Province Engineering_Sub_Market PM SiteID WorkType BuildPlanPriority TotalScore Primary_Drivers ImplementationRequiredDate  Maestro_Site_On_Air Maestro_Site_On_Air_Completed Maestro_Plan_Id Maestro_Cell_Type Maestro_InvestmentDriver TriggeredSiteID Next_Special_Event Next_Special_Event_Date First_Special_Event First_Special_Event_Date
    | rename First_Special_Event as First_Special_Event_In_Plan_Year, First_Special_Event_Date as First_Special_Event_Date_In_Plan_Year
    | replace  "AB" with "Alberta", "BC" with "British Columbia", "PQ" with "Quebec", "Ontario Eastern" with "Ontario", "ON" with "Ontario" in Province
    | sort -TotalScore"""

    query_current_year_site_level = """| loadjob savedsearch="scheduler_capacity:telus_whsia:Build Plan Ranking"
    | where Maestro_Site_On_Air_Completed = 0 AND Maestro_Plan_Id = tonumber(strftime(now(),"%Y")) 
    | replace "ASAP - Current Slow Sector" with -1 in ImplementationRequiredDate
    | stats max(Province) as Province max(Engineering_Sub_Market) as Engineering_Sub_Market  values(PM) as PM values(WorkType) as WorkType max(TotalScore) as TotalScore, min(BuildPlanPriority) as BuildPlanPriority  min(ImplementationRequiredDate) as ImplementationRequiredDate min(Maestro_Site_On_Air) as Maestro_Site_On_Air min(Maestro_Site_On_Air_Completed) as Maestro_Site_On_Air_Completed   values(Maestro_Cell_Type) as Maestro_Cell_Type values(Maestro_InvestmentDriver) as Maestro_InvestmentDriver values(TriggeredSiteID) as TriggeredSiteID min(Next_Special_Event) as Next_Special_Event min(Next_Special_Event_Date) as Next_Special_Event_Date min(First_Special_Event) as First_Special_Event min(First_Special_Event_Date) as First_Special_Event_Date min(Driver1) as Driver1 max(Driver2) as Driver2  max(Driver3) as Driver3  by SiteID Maestro_Plan_Id
    | replace -1 with "ASAP - Current Slow Sector" in ImplementationRequiredDate
    | eval Primary_Drivers = coalesce(mvjoin(mvappend(Driver1, Driver2, Driver3), ", "), "Other Factors"), WorkType = mvjoin(WorkType, ", ")
    | table Province Engineering_Sub_Market PM SiteID WorkType BuildPlanPriority TotalScore Primary_Drivers ImplementationRequiredDate  Maestro_Site_On_Air Maestro_Site_On_Air_Completed Maestro_Plan_Id Maestro_Cell_Type Maestro_InvestmentDriver TriggeredSiteID Next_Special_Event Next_Special_Event_Date First_Special_Event First_Special_Event_Date
    | rename First_Special_Event as First_Special_Event_In_Plan_Year, First_Special_Event_Date as First_Special_Event_Date_In_Plan_Year
    | replace  "AB" with "Alberta", "BC" with "British Columbia", "PQ" with "Quebec", "Ontario Eastern" with "Ontario", "ON" with "Ontario" in Province
    | sort -TotalScore"""

    query_next_year_site_level = """| loadjob savedsearch="scheduler_capacity:telus_whsia:Build Plan Ranking"
    | where Maestro_Site_On_Air_Completed = 0 AND Maestro_Plan_Id = tonumber(strftime(now(),"%Y")) + 1
    | replace "ASAP - Current Slow Sector" with -1 in ImplementationRequiredDate
    | stats max(Province) as Province max(Engineering_Sub_Market) as Engineering_Sub_Market  values(PM) as PM values(WorkType) as WorkType max(TotalScore) as TotalScore, min(BuildPlanPriority) as BuildPlanPriority  min(ImplementationRequiredDate) as ImplementationRequiredDate min(Maestro_Site_On_Air) as Maestro_Site_On_Air min(Maestro_Site_On_Air_Completed) as Maestro_Site_On_Air_Completed   values(Maestro_Cell_Type) as Maestro_Cell_Type values(Maestro_InvestmentDriver) as Maestro_InvestmentDriver values(TriggeredSiteID) as TriggeredSiteID min(Next_Special_Event) as Next_Special_Event min(Next_Special_Event_Date) as Next_Special_Event_Date min(First_Special_Event) as First_Special_Event min(First_Special_Event_Date) as First_Special_Event_Date min(Driver1) as Driver1 max(Driver2) as Driver2  max(Driver3) as Driver3  by SiteID Maestro_Plan_Id
    | replace -1 with "ASAP - Current Slow Sector" in ImplementationRequiredDate
    | eval Primary_Drivers = coalesce(mvjoin(mvappend(Driver1, Driver2, Driver3), ", "), "Other Factors"), WorkType = mvjoin(WorkType, ", ")
    | table Province Engineering_Sub_Market PM SiteID WorkType BuildPlanPriority TotalScore Primary_Drivers ImplementationRequiredDate  Maestro_Site_On_Air Maestro_Site_On_Air_Completed Maestro_Plan_Id Maestro_Cell_Type Maestro_InvestmentDriver TriggeredSiteID Next_Special_Event Next_Special_Event_Date First_Special_Event First_Special_Event_Date
    | rename First_Special_Event as First_Special_Event_In_Plan_Year, First_Special_Event_Date as First_Special_Event_Date_In_Plan_Year
    | replace  "AB" with "Alberta", "BC" with "British Columbia", "PQ" with "Quebec", "Ontario Eastern" with "Ontario", "ON" with "Ontario" in Province
    | sort -TotalScore"""

    print("Running Splunk queries")
    df_current_year_activity_level = splunk_to_df(query_current_year_activity_level)
    df_current_year_site_level = splunk_to_df(query_current_year_site_level)
    df_next_year_activity_level = splunk_to_df(query_next_year_activity_level)
    df_next_year_site_level = splunk_to_df(query_next_year_site_level)
    df_field_defintions = pd.read_csv('Build Plan Field Definitions.csv')

    df_next_year_site_level['TotalScore'] = pd.to_numeric(df_next_year_site_level['TotalScore'],downcast='float')
    df_current_year_site_level['TotalScore'] = pd.to_numeric(df_current_year_site_level['TotalScore'],downcast='float')
    df_next_year_activity_level['TotalScore'] = pd.to_numeric(df_next_year_site_level['TotalScore'],downcast='float')
    df_current_year_activity_level['TotalScore'] = pd.to_numeric(df_current_year_site_level['TotalScore'],downcast='float')

    report_date = date.today().strftime("%B_%Y") 
    file_name =  'Capacity_Build_Plan_Ranking_{}.xlsx'.format(report_date)

    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')

    df_current_year_activity_level.to_excel(writer, sheet_name='Current Year - Activity Level', index=False)
    df_current_year_site_level.to_excel(writer, sheet_name='Current Year - Site Level', index=False)
    df_next_year_activity_level.to_excel(writer, sheet_name='Next Year - Activity Level', index=False)
    df_next_year_site_level.to_excel(writer, sheet_name='Next Year - Site Level', index=False)
    df_field_defintions.to_excel(writer, sheet_name='Field Definitions', index=False)


    writer.save()
    print('File saved as {}'.format(file_name))

    ###################
    ## Send email 
    ###################

    user = 'capacity-analytics@telus.com'  
    to =  ['dylan.moss@telus.com']


    subject = 'Capacity Build Plan Ranking - {}'.format(report_date)
    body = 'Please find attached the Capacity Build Plan Ranking for the current and next year as of {}.\n\nThanks.\nThe future is friendly'\
            '\n\n This email, including any attachments, is for the sole use of the intended recipient and contains confidential information.'\
            'If you are not the intended recipient, please notify us immediately and destroy this email and any copies.'.format(report_date)

    msg = MIMEMultipart()
    msg['From'] = user
    msg['To'] = ", ".join(to)
    msg['Subject'] = subject
    msg.attach(MIMEText(body,'plain'))


    ## Attach Build Plan Ranking File
    attachment= open(file_name,'rb')
    part = MIMEBase('application','octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition',"attachment; filename= {}".format(file_name))


    msg.attach(part)
    text = msg.as_string()
    print('Sending Email')

    try:  
        server = smtplib.SMTP('mail.telus.com')
        server.ehlo()
        server.sendmail(user, to, text)
        server.close()
        print('Email sent!')
        
    except:  
        print('Something went wrong...')

if __name__ == "__main__":
    main()

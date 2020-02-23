import pandas as pd
import numpy as np
import time
import pyodbc
import sys

from dateutil.parser import parse

def getHubDF():
    conn = pyodbc.connect('DRIVER={SQL SERVER};SERVER="IP ADDRESS";Database="Database Name";UID="Uid";PWD="Password"')
    SQL_Query = pd.read_sql_query(
    '''SELECT * FROM TableName''', conn)
    return pd.DataFrame(SQL_Query)    

def getDataFrame():
    conn = pyodbc.connect('DRIVER={SQL SERVER};SERVER="IP ADDRESS";Database="Database Name";UID="Uid";PWD="Password"')
    SQL_Query = pd.read_sql_query(
    '''SELECT * FROM TableName''', conn)
    return pd.DataFrame(SQL_Query)

def getDfRowAsList(serial,each_site_df):
    row_list=each_site_df[each_site_df.Serial==serial].values.tolist()
    str1 = ''.join(str(x) for x in row_list)
    row_list=str1.split(",")
    return row_list

def getMinDuration(row_list):
    t1=pd.to_datetime(row_list[2])
    t2=pd.to_datetime(row_list[3])
    return (pd.Timedelta(t2 - t1).seconds / 60.0)


def time_diff(t2,t1):
    t1=pd.to_datetime(t1)
    t2=pd.to_datetime(t2)
    return (pd.Timedelta(t2 - t1).seconds / 60.0)   
   

def handleNullValues():
    df['SiteCode'].replace('', np.nan, inplace=True)
    df['Alarm'].replace('', np.nan, inplace=True)
    df['FirstOccurrence'].replace('', np.nan, inplace=True)
    df['ClearTimestamp'].replace('', np.nan, inplace=True)
    df['FirstOccurrence'].replace("''", np.nan, inplace=True)
    df['ClearTimestamp'].replace("''", np.nan, inplace=True)
    df['FirstOccurrence'].replace(" ''", np.nan, inplace=True)
    df['ClearTimestamp'].replace(" ''", np.nan, inplace=True)
   
    df.dropna(subset=['SiteCode'], inplace=True)
    df.dropna(subset=['Alarm'], inplace=True)
    df.dropna(subset=['FirstOccurrence'], inplace=True)
    df.dropna(subset=['ClearTimestamp'], inplace=True)  

def clearSite(site):
    print("Clearing Site")
    global df
    df=df[df.SiteCode!=site]
    
def check_4hr_mfs(id1,mf_start,each_site_df):
    print("Looking of MF Start With in 4 hours")
    num_of_main=0
    hour=0
    flag=True
    id1=id1-1
    while(flag and id1>0):
        main_list=getDfRowAsList(id1,each_site_df)
        print(main_list)
        hour=time_diff(mf_start,main_list[2])
        if(hour>=240):
            return num_of_main
        else:
            if main_list[1]==" 'Mains Fail'":
                num_of_main=num_of_main+1
                if(num_of_main>1):
                    return num_of_main
            id1=id1-1
    return num_of_main

def check_4hr_mfe(id1,mf_start,each_site_df):
    print("Looking of MF End With in 4 hours")
    num_of_main=0
    hour=0
    flag=True
    id1=id1-1
    while(flag and id1>0):
        main_list=getDfRowAsList(id1,each_site_df)
        print(main_list)
        hour=time_diff(mf_start,main_list[3])
        if(hour>=240):
            return num_of_main
        else:
            if main_list[1]==" 'Mains Fail'":
                num_of_main=num_of_main+1
                if(num_of_main>0):
                    return num_of_main
            id1=id1-1
    return num_of_main

def rule1(idx1,idx2,idx3,each_site_df,each_site):
    global hubDF
    global result
    print("Sequence Matched")
    mf_list=getDfRowAsList(idx1,each_site_df)
    dc_list=getDfRowAsList(idx2,each_site_df)
    sd_list=getDfRowAsList(idx3,each_site_df)
    print(mf_list)
    print(dc_list)
    print(sd_list)
    mf_duration=getMinDuration(mf_list)
    sd_duration=getMinDuration(sd_list)
    sds_mfs_gap=time_diff(sd_list[2],mf_list[2])
    if (sd_duration>=10) and (mf_duration>=60) and (sds_mfs_gap<=720):
        print("First 3 conditions satisfied")
        mfs_count=check_4hr_mfs(idx1,mf_list[2],each_site_df)
        mfe_count=check_4hr_mfs(idx1,mf_list[2],each_site_df)
        print(mfs_count)
        print(mfe_count)
        if (mfs_count==0) and (mfe_count==0):
            print("Found No MF Start or End In the past 4 hours")
            if (abs(time_diff(sd_list[2],mf_list[3]))<=10) or (abs(time_diff(sd_list[3],mf_list[3]))<=10):
                print("All Condition Matched")
                bb=sds_mfs_gap
                new_row = {'SiteCode':each_site,'Result':bb,'Rule':"Rule 01"}
                result=result.append(new_row, ignore_index=True)
                print(result)
        


    

def check_sequence():
    checklist=list()
    checklist.append('BBLMA02')
    global all_site_list
    global df
    for each_site in checklist:      
        each_site_df=df[df.SiteCode==each_site].sort_values('FirstOccurrence')
        if(each_site_df[each_site_df.Alarm=="Site Down"].shape[0]!=0 and each_site_df[each_site_df.Alarm=="DC Low"].shape[0]!=0):
            print("-------Site Name "+str(each_site)+"--------------")
            each_site_df['Serial'] = np.arange(len(each_site_df))
            lastSerial=each_site_df.shape[0]-1
            current_row=-1
            for alarm in each_site_df['Alarm']:
                current_row=current_row+1
                if(alarm=="Mains Fail"):
                    nextRow=getDfRowAsList(current_row+1,each_site_df)
                    nextRow2=getDfRowAsList(current_row+2,each_site_df)
                    if(len(nextRow)==5and len(nextRow2)==5):
                        if (nextRow[1]==" 'DC Low'" and nextRow2[1]==" 'Site Down'"):
                            rule1(current_row,current_row+1,current_row+2,each_site_df,each_site)
            clearSite(each_site)
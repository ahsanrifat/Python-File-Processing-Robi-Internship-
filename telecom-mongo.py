import pandas as pd
import os
import pymongo

#Enter Directory
os.chdir(r'/home/syed/Documents/Folder1')
files=os.listdir()
combined=pd.DataFrame()
for file in files:
    name='/home/syed/Documents/Folder1/{}'.format(file)
    col_name=['MSISDN', 'B_NO', 'DATE_CALL', 'TIME_CALL', 'CELL_ID', 'CELL_ID_LAST','LOC_AREA_ID', 'LOCAL_AREA_ID_LAST']
    df=pd.read_csv(name,names=col_name,low_memory=False)
    combined=combined.append(df, ignore_index=True)

#Enter Directory
os.chdir(r'/home/syed/Documents/Folder2')
files2=os.listdir()
files2
csrm=pd.DataFrame()
for file in files2:
    name='/home/syed/Documents/Folder2/{}'.format(file)
    df=pd.read_csv(name,low_memory=False)
    df.columns=['MSISDN2', 'Complaint type', 'SR Area', 'SR SubArea', 'Complaint time']
    csrm=csrm.append(df, ignore_index=True)
for index,row in csrm.iterrows():
    if(math.isnan(row.MSISDN2)):
        csrm=csrm.drop(index)
csrm["MSISDN"]=0
for index,row in csrm.iterrows():
    if(str(row.MSISDN2)[0:3]=='966'):
        num=str(row.MSISDN2)
        num=num[3:]
        num=num[:-2]
        csrm.at[index,'MSISDN']=num
    else:
        csrm=csrm.drop(index)
del csrm["MSISDN2"]


finalDf=combined.merge(csrm, how='inner',on='MSISDN')

data=finalDf["CELL_ID"].value_counts().sort_values(ascending=False).head(10)
data=dict(data)

cell=pd.DataFrame(data.items(), columns=['CELL_ID', 'COUNT'])
cell['Time']=datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

#inserting data into mongoDB

client = pymongo.MongoClient("MONGO DB CONNECTION STRING HERE")
database = client.Database1
collection1=database.collection1
collection2=database.collection2
collection1.insert_many(finalDf.to_dict('records'))
collection2.insert_many(cell.to_dict('records'))

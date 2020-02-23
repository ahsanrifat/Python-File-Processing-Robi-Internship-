import pandas as pd
import datetime
import time

def getFileName():
    ts = time.time()
    d = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    d="E:\Processed_file"+d+".csv"
    return d

def newest(path):
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)

path=newest(r"E:\New Folder")


col=[str(x) for x in range (0,27)]

df=pd.read_csv((path),names=col,encoding='latin1')

print("Total Number of Row")

print(df.shape[0])

df['7']=df['7'].str.strip()

df2 =df[df['7']=="Internet Pack"]

df2 =df2[df2['24'].str.contains('in process')==False]

print("After Filtering The Total Number Of Row")

print(df2.shape[0])

destination=getFileName()

df2.to_csv(r'{}'.format(destination))

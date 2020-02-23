import pandas as pd
import datetime
import sys
import time
import os
import tarfile

#generating processed file's name based on date and time
def getFileName(fileDirectory):
    ts = time.time()
    d = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    #replacing 12:12:12 to 12_12_12 as file name can not contain : 
    d=fileDirectory+"\\{}".format(d.replace(":","_"))+".csv"
    return d

#for getting the latest file's path from the stored file's folder
def fetchFile(path):
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)

#for unzipping file
def unzip_file(path):
    if path.endswith("tar.gz"):
        tar = tarfile.open(path, "r:gz")
        tar.extractall(source)
        tar.close()
    elif path.endswith("tar"):
        tar = tarfile.open(path, "r:")
        tar.extractall(source)
        tar.close()

#for reading a text file. Each line is strored as a list item
def read_text_file(path):
    
    
    with open(path) as f:
        lines = f.readlines()
    
    final_list=list()
    for each_line in lines:
        if each_line[0].isdigit():
            number=(each_line[0:26])
            number=number.replace(".","")
            number=number[::-1]
            final_list.append(number)
            
    return final_list

#for converting list to csv 
def aList_to_csv(final_list):
    
    df = pd.DataFrame (final_list,columns=['Numbers'])
    df.to_csv(getFileName(destination), encoding='utf-8', index=False)
    

#---------------------------------------provide source file's location--------------------------------------------
#getting the path to read log file
source=r"E:\zip2"
#------------------------------------------------------------------------------------------------------------------

#---------------------------------------provide destination file's location--------------------------------------------
#getting the path to read log file
destination=r"E:\Processed_file"
#------------------------------------------------------------------------------------------------------------------

while (len(os.listdir(source))!=0):
    
    
    zipped_file=fetchFile(source)
    
    unzip_file(zipped_file)
    
    os.remove(zipped_file)
    
    txt_file=fetchFile(source)
    
    final_number_list=read_text_file(txt_file)
    
    aList_to_csv(final_number_list)
    
    os.remove(txt_file)
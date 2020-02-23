import pandas as pd
df=pd.DataFrame()
for x in range(0, 5):
    d = pd.read_json('JSON LINK HERE')
    d = pd.DataFrame(d.values.tolist())
    df = pd.concat([df, d], ignore_index=True, sort =False)
    
print((df[29].value_counts()/df[29].count())*100)
print(df[29].unique())

#this program returns the percentage of successful and dropped call rates of a user for last 6 days
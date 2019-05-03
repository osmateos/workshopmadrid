# Databricks notebook source
start_year = 2018

# COMMAND ----------

import zipfile
import datetime
 
# Generate the list of quarterly zip files archived in EDGAR since
# start_year (earliest: 1993) until the most recent quarter
current_year = datetime.date.today().year
current_quarter = (datetime.date.today().month - 1) // 3 + 1
#start_year = 2018
years = list(range(start_year, current_year))
quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
history = [(y, q) for y in years for q in quarters]
for i in range(1, current_quarter -1):
    history.append((current_year, 'QTR%d' % i))
quarterly_files = ['https://www.sec.gov/Archives/edgar/full-index/%d/%s/master.zip' % (x[0], x[1]) for x
                   in history]
quarterly_files.sort()

# COMMAND ----------

import urllib.request
opener=urllib.request.build_opener()
opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
urllib.request.install_opener(opener)
count=0
for contenturl in quarterly_files:
    print (contenturl)
    count= count+1
    filename = contenturl[45:].replace('/', '_')
    print(filename)
    #try:
    f = open( filename,'wb')
    f.write(urllib.request.urlopen(contenturl).read())
    f.close()
    with zipfile.ZipFile(filename).open('master.idx') as z:
        for i in range(11):
            myarray=z.readline()
        print (myarray)
        records = [tuple(line.decode('latin-1').rstrip().split('|')) for
                       line in z]
        print(records)

# COMMAND ----------

from pyspark.sql import *

listfilestodownload=[]

for k in records:
  listfilestodownload.append(Row(url="https://www.sec.gov/Archives/" +k[4], file=k[4].split('/')[3]))

df = spark.createDataFrame(listfilestodownload)
display(df)

# COMMAND ----------

dbutils.fs.mkdirs('/mnt/adlgen1/test-data/2019')
dbutils.fs.ls('/mnt/adlgen1/test-data')


# COMMAND ----------

def download(url, file, year):
  import urllib.request
  
  with open('/dbfs/mnt/adlgen1/test-data/' + str(year) + '/' + file, 'wb') as f:
    f.write(urllib.request.urlopen(url).read())
  
# spark.udf.register("udfDownload", download)

download('https://www.sec.gov/Archives/edgar/data/1000045/0001193125-18-343069.txt', '0001193125-18-343069.txt', 2019)

# COMMAND ----------

# so now 
df.rdd.map(lambda r: download(r.url, r.file, 2019)).take(10)

dbutils.fs.ls('/mnt/adlgen1/test-data/2019')
#df.rdd.take(1)[0].file

# COMMAND ----------

dbutils.fs.head("dbfs:/mnt/adlgen1/test-data/2019/0000919574-18-007099.txt")

# COMMAND ----------



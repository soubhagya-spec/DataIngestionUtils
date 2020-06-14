def ReadMysql(spark,jdbcDict,PushdownQuery):
  connectionUrl = "jdbc:mysql://{0}/{1}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC" \
                    .format(jdbcDict['jdbcHostname'], jdbcDict['jdbcDatabasename'])
  connectionProp = {
    "user" : jdbcDict['jdbcUserName'],
    "password" : jdbcDict['jdbcPassword'],
    "driver" : "com.mysql.jdbc.Driver"
   }
  pdq = "({0}) pdq".format(PushdownQuery)
  BronzeDf = spark.read.jdbc(url=connectionUrl, table = pdq , properties = connectionProp)
  return (BronzeDf)


def ReadGoogleDrive(spark,ConnectionDict):
  file_id = ConnectionDict["FileId"]
  destination = '/tmp/GoogleDriveFile'
  download_file_from_google_drive(file_id, destination)
  dbutils.fs.mkdirs("dbfs:/test/")
  dbutils.fs.mv('file:/tmp/GoogleDriveFile', "dbfs:/test/")
  BronzeDf = spark.read.format("csv").option("header","true").option("delimiter",ConnectionDict["Delimiter"]).option(
    "inferSchema", "false").load("dbfs:/test/GoogleDriveFile")
  #dbutils.fs.rm("dbfs:/test/GoogleDriveFile",True)
  dbutils.fs.rm("file:/tmp/GoogleDriveFile",True)
  return BronzeDf

def download_file_from_google_drive(id, destination):
    import requests
    URL = "https://docs.google.com/uc?export=download"
    session = requests.Session()
    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = None
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            token = value
    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)
    CHUNK_SIZE = 32768
    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

def ReadOneDrive(spark,ConnectionDict):
  URl= ConnectionDict["Link"]
  destination = '/tmp/OneDriveFile'
  download_file_from_one_drive(URl, destination)
  dbutils.fs.mkdirs("dbfs:/test/")
  dbutils.fs.mv('file:/tmp/OneDriveFile', "dbfs:/test/")
  BronzeDf = spark.read.format("csv").option("header","true").option("delimiter",ConnectionDict["Delimiter"]).option(
    "inferSchema", "false").load("dbfs:/test/OneDriveFile")
  #dbutils.fs.rm("dbfs:/test/OneDriveFile",True)
  try:
    dbutils.fs.rm("file:/tmp/OneDriveFile",True)
  except:
	message = " "
  return BronzeDf

def download_file_from_one_drive(URL, destination):
    import requests
    URL = URL
    session = requests.Session()
    response = session.get(URL, stream = True)
    token = None
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            token = value
    if token:
        params = {  'confirm' : token }
        response = session.get(URL, params = params, stream = True)
    CHUNK_SIZE = 32768
    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-
                f.write(chunk)
                
                
def ReadHive(spark,odbcDict,PushdownQuery):
  import pyodbc
  import time as time
  import json
  import os
  import urllib
  import warnings
  import re
  import pandas as pd
 
  #Create the connection to Hive using ODBC
  SERVER_NAME = odbcDict['odbcHostname']
  DATABASE_NAME = odbcDict['odbcDatabasename']
  USERID = odbcDict['odbcUserName']
  PASSWORD = odbcDict['odbcPassword']
  DB_DRIVER="//usr/lib/hive/lib/native/Linux-amd64-64/libhortonworkshiveodbc64.so"  
  driver = 'DRIVER={' + DB_DRIVER + '}'
  server = 'Host=' + SERVER_NAME + ';Port=443'
  database = 'Schema=' + DATABASE_NAME
  hiveserv = 'HiveServerType=2'
  auth = 'AuthMech=6'
  uid = 'UID=' + USERID
  pwd = 'PWD=' + PASSWORD
  CONNECTION_STRING = ';'.join([driver,server,database,hiveserv,auth,uid,pwd])
  connection = pyodbc.connect(CONNECTION_STRING, autocommit=True)
  queryString = """
      {0} ;
  """.format(PushdownQuery)

  pandas_df = pd.read_sql(queryString,connection)
  BronzeDf = sqlContext.createDataFrame(pandas_df)              
  return BronzeDf              
                
    
def ReadAdlsG1(spark,ConnectionDict):
  spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
  spark.conf.set("dfs.adls.oauth2.client.id", "{0}".format(ConnectionDict["ApplicationId"]))
  spark.conf.set("dfs.adls.oauth2.credential","{0}".format(ConnectionDict[""]))
  spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/{0}/oauth2/token".format(ConnectionDict[""]))
  if(ConnectionDict["Format"] == "Parquet"):
    BronzeDf = spark.read.format("parquet").option("header", "true").load(ConnectionDict["Path"])
  elif(ConnectionDict["Format"] == "csv"):
    BronzeDf = spark.read.format("csv").option("header", "true").option("delimiter",ConnectionDict["Delimiter"]).load(ConnectionDict["Path"])
  elif(ConnectionDict["Format"] == "ORC"):
    BronzeDf = spark.read.format("orc").option("header", "true").load(ConnectionDict["Path"])
  else:
    BronzeDf = None
  return BronzeDf    


def ReadBlob(spark,ConnectionDict):
  spark.conf.set(
    "fs.azure.account.key.{0}.blob.core.windows.net".format(ConnectionDict["StorageAccountName"]),ConnectionDict["StorageAccountAccessKey"])
  path = "wasb://{0}@{1}.blob.core.windows.net/".format(ConnectionDict["ContainerName"],ConnectionDict["StorageAccountName"])
  if(ConnectionDict["Format"] == "Parquet"):
    BronzeDf = spark.read.format("parquet").option("header", "true").load(path + ConnectionDict["Path"])
  elif(ConnectionDict["Format"] == "csv"):
    BronzeDf = spark.read.format("csv").option("header", "true").option(
      "delimiter",ConnectionDict["Delimiter"]).load(path + ConnectionDict["Path"])
  elif(ConnectionDict["Format"] == "ORC"):
    BronzeDf = spark.read.format("orc").option("header", "true").load(path + ConnectionDict["Path"])
  else:
    BronzeDf = None
  return BronzeDf
    
    
def ReadFileFromSource(spark,cnx,entryid,jdbcUrl,connectionProperties):
  pdq_1 = "(select * from `deaccelator`.`datacatlogentry` where `EntryID` = {0} ) pdq_1".format(entryid)
  pdq_2= "(select * from `deaccelator`.`parameter` where `EntryID` = {0} ) pdq_2".format(entryid)
  pdq_3= "(select * from `deaccelator`.`metadata` where `EntryID` = {0} ) pdq_2".format(entryid) 
  datacatlogentry = spark.read.jdbc(url=jdbcUrl, table = pdq_1, properties = connectionProperties)
  parameter = spark.read.jdbc(url=jdbcUrl, table  = pdq_2, properties = connectionProperties)
  MetadataDf = spark.read.jdbc(url  =jdbcUrl, table  = pdq_3, properties = connectionProperties)
  from pyspark.sql.functions import regexp_replace
  MetadataDf = MetadataDf.withColumn("ColumnName",regexp_replace(MetadataDf["ColumnName"]," ",""))
  CatlogDict = datacatlogentry.first().asDict()
  ParameterDict = parameter.first().asDict()
  columns = MetadataDf.select(MetadataDf['ColumnName']).collect()
  column_list = []
  for x in columns:
    column_list.append((x.ColumnName).replace(" ",""))
  from datetime import datetime
  dateTimeObj = (datetime.now())
  start_time = dateTimeObj.strftime("%Y-%m-%d %H-%M-%S.%f")
  cur = cnx.cursor()
  querystring ="INSERT INTO `deaccelator`.`audittable` (`EntryID`,`JobName`,`ProjectName`,`StartTime`,`UserName`,`Status`)VALUES ({0},'{1}','{2}','{3}','{4}','Running')".format(
    CatlogDict["EntryID"],CatlogDict["JobName"],CatlogDict["ProjectName"],start_time,CatlogDict["UserName"])
  cur.execute(querystring)
  cnx.commit()
  cur.close()
  BronzeDf = ReadFile(spark,CatlogDict,ParameterDict,column_list)
  return BronzeDf,MetadataDf
  
  
def ReadFile(spaark,CatlogDict,ParameterDict,ColumnList):
  ConnectionDict =  eval(ParameterDict["SourceParameter"])
  if(CatlogDict["SourceType"]== "MySql" ):
    PushdownQuery  = ParameterDict["SourceQuery"]
    BronzeDf = ReadMysql(spark,ConnectionDict,PushdownQuery)
  elif (CatlogDict["SourceType"]== "Google Drive" ):
    BronzeDf = ReadGoogleDrive(spark,ConnectionDict)
  elif (CatlogDict["SourceType"]== "One Drive" ):
    BronzeDf = ReadOneDrive(spark,ConnectionDict)
  elif (CatlogDict["SourceType"]== "Hive" ):
    PushdownQuery  = ParameterDict["SourceQuery"]
    BronzeDf = ReadHive(spark,ConnectionDict,PushdownQuery)
  elif (CatlogDict["SourceType"]== "ADLS Gen 1" ):
    BronzeDf = ReadAdlsG1(spark,ConnectionDict)
  elif (CatlogDict["SourceType"]== "AzureBlob" ):
    BronzeDf = ReadBlob(spark,ConnectionDict)
  else:
    BronzeDf = None
  if(BronzeDf != None):
    BronzeDf = BronzeDf.toDF(*ColumnList)
  return BronzeDf
  
  
def typecast(spark,BronzeDf,metadata_dict):
    import pyspark.sql.functions as f
    if(BronzeDf.filter(BronzeDf["Flag"].contains("Tier 1")).count() == 0):
        SilverDf = BronzeDf.filter(~(BronzeDf["Flag"].contains("Tier 2")))
        RejectedDf = BronzeDf.filter(BronzeDf["Flag"].contains("Tier 2")).drop("Flag")
        for key,value in metadata_dict.items():
          col_dict = metadata_dict[key]
          if (col_dict["DQCheck"] == "Tier 2"):
              if(col_dict["DataType"] == "int"):
                  SilverDf  = SilverDf.withColumn(key,SilverDf[key].cast("int"))
              elif(col_dict["DataType"] == "double" or col_dict["DataType"] == "float") :
                  SilverDf  = SilverDf.withColumn(key,SilverDf[key].cast("double"))
              elif(col_dict["DataType"] == "datetime" ):
                  format_date = formatconversion(col_dict["Format"])
                  SilverDf  = SilverDf.withColumn(key,f.to_date(SilverDf[key],format_date))
              elif(col_dict["DataType"] == "timestamp" ):
                  format_date = formatconversion(col_dict["Format"])
                  SilverDf = SilverDf.withColumn(key,f.to_timestamp(SilverDf[key],format_date))		
                
        if(SilverDf.filter(SilverDf["Flag"].contains("Tier 3")).count() == 0):
            SilverDf = SilverDf.drop("Error_Column","Error_Message","Flag")
        else:
            SilverDf = SilverDf.drop("Flag")
    else:
        RejectedDf = BronzeDf.drop("Error_Column","Error_Message","Flag")
        SilverDf = spark.createDataFrame([],RejectedDf.schema)
    return SilverDf,RejectedDf
	 
    
    
  
def CheckDuplicates(spark,entryid,cnx,BronzeDf,MetadataDf):
  primary_key = MetadataDf.select(MetadataDf['ColumnName']).filter(MetadataDf['Primarykey'] == 'Yes')
  TotalRows = BronzeDf.count()
  BronzeDf = BronzeDf.dropDuplicates()
  DuplicateRecords = TotalRows -  BronzeDf.count()
  if(primary_key.count() !=0):
    primary_key = (primary_key.first()).asDict()
    PrimaryKeyColumn=primary_key["ColumnName"]
    BronzeDf = BronzeDf.dropDuplicates([PrimaryKeyColumn])
    DuplicatePrimaryKey = TotalRows -  BronzeDf.count()
    DuplicateRecords = DuplicateRecords + DuplicatePrimaryKey
  DQCheckFailed = BronzeDf.filter(~(BronzeDf["Flag"] == '')).count()
  cur = cnx.cursor()
  querystring ="update `deaccelator`.`audittable` set TotalRows={0},DuplicateRecords={1},DQCheckFailed={2} where EntryID = {3}".format(TotalRows,DuplicateRecords,DQCheckFailed,entryid)
  cur.execute(querystring)
  cnx.commit()
  cur.close()
  return BronzeDf

    
def formatconversion(format_date):
    format_for_date = format_date.replace("%a","E").replace("%A","EEEE").replace("%d","dd").replace(
        "%-d","d").replace("%B","MMMM").replace("%b","MMM").replace("%m","MM").replace("%-m","M").replace(
        "%y","yy").replace("%Y","yyyy").replace("%H","kk").replace("%-H","k").replace("%I","KK").replace("%-I","K").replace(
        "%p","aa").replace("%M","mm").replace("%-M","m").replace("%S","ss").replace("%-S","s").replace("%Z","zz").replace(
        "%W","ww")
    return format_for_date
	

def WriteFileToTarget(spark,cnx,entryid,jdbcUrl,connectionProperties,SilverDf,RejectedDf):
  pdq_1 = "(select * from `deaccelator`.`datacatlogentry` where `EntryID` = {0} ) pdq_1".format(entryid)
  pdq_2= "(select * from `deaccelator`.`parameter` where `EntryID` = {0} ) pdq_2".format(entryid)
  datacatlogentry = spark.read.jdbc(url=jdbcUrl, table = pdq_1, properties = connectionProperties)
  parameter = spark.read.jdbc(url=jdbcUrl, table  = pdq_2, properties = connectionProperties)
  CatlogDict = datacatlogentry.first().asDict()
  ParameterDict = parameter.first().asDict()
  if(CatlogDict["TargetType"]== "ADLS Gen 1" ):
    path = WriteAzure(spark,SilverDf,RejectedDf,ParameterDict,CatlogDict)
  else:
    IngestionMessage =  "Unsupported Target"
    path = ''
  IngestedRows = SilverDf.count()
  RejectedRows = RejectedDf.count()
  cur = cnx.cursor()
  querystring ="update `deaccelator`.`audittable` set IngestedRows = '{0}',RelativeFilePath = '{1}',RejectedRows = '{2}' where EntryId = {3}".format(
    IngestedRows,path,RejectedRows,entryid)
  cur.execute(querystring)
  cnx.commit()
  cur.close()
  
    
def WriteAzure(spark,SilverDf,RejectedDf,ParameterDict,CatlogDict):
  import pyspark.sql
  from datetime import datetime
  ConnectionDict =  eval(ParameterDict["TargetParameter"])
  spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
  spark.conf.set("dfs.adls.oauth2.client.id", "{0}".format(ConnectionDict["ApplicationID"]))
  spark.conf.set("dfs.adls.oauth2.credential","{0}".format(ConnectionDict["ApplicationCredential"]))
  spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/{0}/oauth2/token".format(ConnectionDict["DirectoryID"]))
  dateTimeObj = (datetime.now())
  current_time = dateTimeObj.strftime("%H-%M-%S.%f")
  date_path ="YYYY=" +dateTimeObj.strftime("%Y") + ("/MM=") + dateTimeObj.strftime("%m") + ("/DD=") + dateTimeObj.strftime("%d") 
  output_path = "adl://{0}.azuredatalakestore.net/{1}/{2}/{3}/OUTPUT/".format(
    ConnectionDict["adlAccountName"],CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"]) + date_path 
  reject_path = "adl://{0}.azuredatalakestore.net/{1}/{2}/{3}/REJECT/".format(
    ConnectionDict["adlAccountName"],CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"])+ date_path  + "/Rejection_" + current_time
  IngestionMessage = "Succesful"
  if(ParameterDict["TargetFileType"] == "Parquet"):
    SilverDf.write.format("parquet").mode(CatlogDict["Operation"]).option("header", "true").save(output_path)
  elif(ParameterDict["TargetFileType"] == "Flatfiles"):
    SilverDf.write.format("csv").mode(CatlogDict["Operation"]).option(
      "header", "true").option("delimiter",ParameterDict["TargetFileDelimiter"]).save(output_path)
  elif(ParameterDict["TargetFileType"] == "ORC"):
    SilverDf.write.format("orc").mode(CatlogDict["Operation"]).option("header", "true").save(output_path)
  else:
    IngestionMessage = "Unsuccesful"
  out_path = "/{0}/{1}/{2}/OUTPUT/".format(
    CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"]) + date_path
  rej_path = "/{0}/{1}/{2}/REJECT/".format(
    CatlogDict["ProjectCategory"], CatlogDict["ProjectName"],CatlogDict["JobName"]) + date_path  +"/Rejection_" + current_time  
  if(RejectedDf.count()!= 0):
    RejectedDf.coalesce(1).write.format("csv").mode(CatlogDict["Operation"]).option("header", "true").save(reject_path)
    if(SilverDf.count() != 0):
      path = "OUTPUT PATH : {0} | REJECT OUTPUT PATH : {1}".format(out_path,rej_path)
    else:
      path = "REJECT OUTPUT PATH : {0}".format(rej_path)
  else:
    path = "OUTPUT PATH : {0}".format(out_path)
  return path
  

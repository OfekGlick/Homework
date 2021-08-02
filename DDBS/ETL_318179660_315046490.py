import time
import findspark
import pandas as pd

findspark.init()
import pyodbc
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
from time import sleep
from pyspark.sql.types import *
from pyspark.sql.window import Window


def init_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def DBconnect(username):
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=technionddscourse.database.windows.net;'
                          'DATABASE={' + username + '};'
                                                    'UID={' + username + '};'
                                                                         'PWD=Qwerty12!')
    cursor = conn.cursor()
    return cursor


cursor = DBconnect("ofek0glick")

# environment data
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4," \
                             "com.microsoft.azure:spark-mssql-connector:1.0.2 pyspark-shell"
# KAFKA DATA
spark, sc = init_spark('ofek')
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'

# SQL SERVER DATA
server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
database_name = "ofek0glick"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "Stage1"
username = "ofek0glick"
password = "Qwerty12!"

# Define the schema of the data:
noaa_schema = StructType([StructField('StationId', StringType(), False),
                          StructField('Date', StringType(), False),
                          StructField('Variable', StringType(), False),
                          StructField('Value', IntegerType(), False),
                          StructField('M_Flag', StringType(), True),
                          StructField('Q_Flag', StringType(), True),
                          StructField('S_Flag', StringType(), True),
                          StructField('ObsTime', StringType(), True)])

stations = pd.read_fwf("ghcnd-stations.txt", colspecs=[(0, 11), (13, 20), (22, 30), (32, 37)], header=None)
stations.columns = ['StationId', 'Elevation']

string_value_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", "IS, GM") \
    .option("maxOffsetsPerTrigger", 100000) \
    .option("startingOffsets", "earliest") \
    .load().selectExpr("CAST(value AS STRING)")
json_df = string_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
streaming_df = json_df.select("json.*")


def handle_batch(batch_df, batch_id):
    start = time.time()
    batch_df = batch_df.drop("M_Flag", "Q_Flag", "S_Flag", "ObsTime")
    batch_df = batch_df.withColumn("Year_Rec", F.expr("substring(Date,0,4)")).withColumn("Month_Rec", F.expr(
        "substring(Date,5,2)")).drop("Date")
    df_x = batch_df.groupBy("StationId", "Year_Rec", "Month_Rec", "Variable").agg(
        F.sum(F.col("Value")).alias("batch_sum"),
        F.count("*").alias("batch_count"))
    try:
        df_x.withColumn("batchId", F.lit(batch_id)) \
            .write \
            .format("jdbc") \
            .mode("append") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", password) \
            .save()
    except ValueError as error:
        print("Connector write failed", error)
    print(str(batch_id) + " took " + str(time.time() - start) + " seconds")


query = streaming_df \
    .writeStream \
    .trigger(processingTime='30 seconds') \
    .foreachBatch(handle_batch) \
    .start() \
    .awaitTermination()
print("done")


def groupby_batchid():
    cursor = DBconnect("ofek0glick")
    cursor.execute("""CREATE TABLE Stage2
                    AS (select StationId, Year_Rec, Month_Rec, Variable, total_sum/total_count Average
                    from(select StationId, Year_Rec, Month_Rec, Variable, SUM(batch_sum) total_sum, SUM(batch_count) total_count
                    from Stage1
                    group by StationId, Year_Rec, Month_Rec, Variable))
                    """)

# com.microsoft.sqlserver.jdbc.spark

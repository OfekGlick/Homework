import time
# import findspark
#
# findspark.init()
import pandas as pd
import pyspark.sql
# import pyodbc
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
from time import sleep
from pyspark.sql.types import *

# environment data
# os.environ[
#     'PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4," \
#                              "com.microsoft.azure:spark-mssql-connector:1.0.2 pyspark-shell"


def init_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


#
# def DBconnect(username):
#     conn = pyodbc.connect('DRIVER={SQL Server};'
#                           'SERVER=technionddscourse.database.windows.net;'
#                           'DATABASE={' + username + '};'
#                                                     'UID={' + username + '};'
#                                                                          'PWD=Qwerty12!')
#     cursor = conn.cursor()
#     return cursor


# SQL SERVER DATA
server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
database_name = "ofek0glick"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "Stage1_Real"
username = "ofek0glick"
password = "Qwerty12!"


def add_additional_data():
    stations = spark.read.text('ghcnd-stations.txt').withColumn("StationId", F.expr("substring(value,0,11)")) \
        .withColumn("Latitude", F.expr("substring(value,12,9)").cast(DoubleType())) \
        .withColumn("Longitude", F.expr("substring(value,22,9)").cast(DoubleType())) \
        .withColumn("Elevation", F.expr("substring(value,32,6)").cast(DoubleType())) \
        .drop("value")
    stations = stations.filter((F.col("StationId").startswith("SW")) | (F.col("StationId").startswith("GM")))
    print("\tloading geospatial data 1 into sql")
    try:
        stations.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", url) \
            .option("dbtable", 'StationData') \
            .option("user", username) \
            .option("password", password) \
            .save()
    except ValueError as error:
        print("Connector write failed", error)


def kafka_stream():
    # Define the schema of the data:
    noaa_schema = StructType([StructField('StationId', StringType(), False),
                              StructField('Date', StringType(), False),
                              StructField('Variable', StringType(), False),
                              StructField('Value', IntegerType(), False),
                              StructField('M_Flag', StringType(), True),
                              StructField('Q_Flag', StringType(), True),
                              StructField('S_Flag', StringType(), True),
                              StructField('ObsTime', StringType(), True)])

    string_value_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", "GM, SW") \
        .option("maxOffsetsPerTrigger", 1500000) \
        .option("startingOffsets", "earliest") \
        .load().selectExpr("CAST(value AS STRING)")
    json_df = string_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
    streaming_df = json_df.select("json.*").limit(150000000)
    vars = {'PRCP', 'SNWD', 'TMAX', 'TMIN'}

    def handle_batch(batch_df: pyspark.sql.DataFrame, batch_id):
        start = time.time()
        batch_df = batch_df.filter(batch_df.Q_Flag.isNull()).drop("M_Flag", "Q_Flag", "S_Flag", "ObsTime")
        batch_df = batch_df.withColumn("Year_Rec", F.expr("substring(Date,0,4)").cast(IntegerType())).withColumn(
            "Month_Rec", F.expr("substring(Date,5,2)").cast(IntegerType())).drop("Date")
        batch_df = batch_df.filter(batch_df.Year_Rec >= 1900)
        batch_df = batch_df.filter(
            ((batch_df.Variable == "PRCP") & (batch_df.Value >= 0)) | (batch_df.Variable == "TMAX") | (
                    batch_df.Variable == "TMIN") | (
                    (batch_df.Variable == "SNWD") & (batch_df.Value >= 0)))
        ger_swe_df = batch_df.groupBy("StationId", "Year_Rec", "Month_Rec", "Variable").agg(
            F.sum(F.col("Value")).alias("batch_sum"),
            F.count("*").alias("batch_count"))
        ger_swe_df = ger_swe_df.groupby("StationId", "Year_Rec", "Month_Rec").pivot("Variable") \
            .agg(F.first("batch_count").cast(LongType()).alias('batch_count'),
                 F.first("batch_sum").cast(LongType()).alias('batch_sum'))
        cols = {str(col)[:4] for col in ger_swe_df.columns if str(col)[:4] in vars}
        missing = vars - cols
        for var in missing:
            ger_swe_df = ger_swe_df.withColumn(var + '_batch_count', F.lit(None).cast(LongType())).withColumn(
                var + '_batch_sum', F.lit(None).cast(LongType()))
        ger_swe_df = ger_swe_df.select("StationId", "Year_Rec", "Month_Rec", "PRCP_batch_count", 'PRCP_batch_sum',
                                       'TMAX_batch_count', 'TMAX_batch_sum', 'TMIN_batch_count', 'TMIN_batch_sum',
                                       'SNWD_batch_count', 'SNWD_batch_sum')
        # fragments = ger_swe_df.select('Year_Rec').distinct().collect()
        # fragments = [list(year) for year in fragments]
        # fragments = [str(year[0])[:3] for year in fragments]
        print("\t\t writing Ger_Swe batch " + str(batch_id) + " to sql")
        # for frag in fragments:
        #     ger_swe_df.filter(F.col("Year_Rec").startswith(frag)).withColumn("batchId", F.lit(batch_id)) \
        #         .write \
        #         .format("com.microsoft.sqlserver.jdbc.spark") \
        #         .mode("append") \
        #         .option("url", url) \
        #         .option("dbtable", "Ger_Swe_Stage1" + frag + "0s") \
        #         .option("user", username) \
        #         .option("password", password) \
        #         .save()
        ger_swe_df.withColumn("batchId", F.lit(batch_id)) \
            .write \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .mode("append") \
            .option("url", url) \
            .option("dbtable", "Ger_Swe") \
            .option("user", username) \
            .option("password", password) \
            .save()
        print("\t\t" + str(batch_id) + " took " + str(time.time() - start) + " seconds")

    query = streaming_df \
        .writeStream \
        .trigger(processingTime='30 seconds') \
        .foreachBatch(handle_batch) \
        .start() \
        .awaitTermination(7200)


# def groupby_batchid():
#     cursor = DBconnect("ofek0glick")
#     cursor.execute("""Select * into Stage2_Ofek  from
#                         (select StationId, Year_Rec, Month_Rec,
#                                 PRCP_SUM/PRCP_COUNT PRCP,
#                                 (TMAX_SUM/TMAX_COUNT)/10 TMAX,
#                                 (TMIN_SUM/TMAX_COUNT)/10 TMIN,
#                                 SNWD_SUM/SNWD_COUNT SNWD
#                         from
#                             (SELECT StationId, Year_Rec, Month_Rec,
#                                     sum(PRCP_batch_sum) PRCP_SUM, sum(PRCP_batch_count) PRCP_COUNT,
#                                     sum(TMAX_batch_sum) TMAX_SUM, sum(TMAX_batch_count) TMAX_COUNT,
#                                     sum(TMIN_batch_sum) TMIN_SUM, sum(TMIN_batch_count) TMIN_COUNT,
#                                     sum(SNWD_batch_sum) SNWD_SUM, sum(SNWD_batch_count) SNWD_COUNT
#                             FROM
#                             Ger_Swe_Stage1_Ofek
#                                 GROUP BY StationId, Year_Rec, Month_Rec) temp
#                         ) temp2 """)
#     cursor.commit()
#     cursor.close()


if __name__ == '__main__':
    spark, sc = init_spark('ofek')
    kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
    print("Kafka started streaming:")
    kafka_stream()
    print("\tKafka data streamed")
    print("Grouping and pivoting")
    # groupby_batchid()
    print("\tGrouped by batchID, also preformed PIVOT")
    print("Adding Geospatial data to sql")
    # add_additional_data()
    print("\tGeospatial data now in sql")
    print("")
    print("done")

# com.microsoft.sqlserver.jdbc.spark
# https://noamcluster1hdistorage.blob.core.windows.net//ex2/ghcnd-stations.txt

import time
import findspark

#
findspark.init()
import pandas as pd
import pyspark.sql
import pyodbc
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
from time import sleep
from pyspark.sql.types import *

# environment data
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4," \
                             "com.microsoft.azure:spark-mssql-connector:1.0.2 pyspark-shell"


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


# SQL SERVER DATA
server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
database_name = "ofek0glick"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "Stage1"
username = "ofek0glick"
password = "Qwerty12!"


def add_additional_data():
    stations = spark.read.text('ghcnd-stations.txt').withColumn("StationId", F.expr("substring(value,0,11)")) \
        .withColumn("Latitude", F.expr("substring(value,12,9)").cast(DoubleType())) \
        .withColumn("Longitude", F.expr("substring(value,22,9)").cast(DoubleType())) \
        .withColumn("Elevation", F.expr("substring(value,32,6)").cast(DoubleType())) \
        .drop("value")
    stations = stations.filter((F.col("StationId").startswith("SW")) | (F.col("StationId").startswith("GM")) | (
        F.col("StationId").startswith("IN")) | (F.col("StationId").startswith("AS")))
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
        .option("subscribe", "GM, SW, IN") \
        .option("maxOffsetsPerTrigger", 1500000) \
        .option("startingOffsets", "earliest") \
        .load().selectExpr("CAST(value AS STRING)")
    json_df = string_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
    streaming_df = json_df.select("json.*").limit(100000000)

    def handle_batch(batch_df: pyspark.sql.DataFrame, batch_id):
        start = time.time()
        batch_df = batch_df.filter(batch_df.Q_Flag.isNull()).drop("M_Flag", "Q_Flag", "S_Flag", "ObsTime")
        batch_df = batch_df.withColumn("Year_Rec", F.expr("substring(Date,0,4)").cast(IntegerType())).withColumn(
            "Month_Rec", F.expr("substring(Date,5,2)").cast(IntegerType())).drop("Date")
        batch_df = batch_df.filter(
            ((batch_df.Variable == "PRCP") & (batch_df.Value >= 0)) | (batch_df.Variable == "TMAX") | (
                        batch_df.Variable == "TMIN") | (
                    (batch_df.Variable == "SNWD") & (batch_df.Value >= 0)))
        batch_df = batch_df.filter(batch_df.Year_Rec >= 1850)
        batch_df = batch_df.groupBy("StationId", "Year_Rec", "Month_Rec", "Variable").agg(
            F.sum(F.col("Value")).alias("batch_sum"),
            F.count("*").alias("batch_count"))
        try:
            batch_df.withColumn("batchId", F.lit(batch_id)) \
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
        print("\t\t" + str(batch_id) + " took " + str(time.time() - start) + " seconds")

    query = streaming_df \
        .writeStream \
        .trigger(processingTime='30 seconds') \
        .foreachBatch(handle_batch) \
        .start() \
        .awaitTermination(3600)


def groupby_batchid():
    cursor = DBconnect("ofek0glick")
    cursor.execute("""Select * into Stage2  from
                        (SELECT StationId, Year_Rec, Month_Rec,
                          [PRCP], [TMAX], [TMIN]
                        FROM
                        (select temp.StationId, temp.Year_Rec, temp.Month_Rec, temp.Variable, temp.total_sum/temp.total_count Average
                        from
                            (select StationId, Year_Rec, Month_Rec, Variable, SUM(batch_sum) total_sum, SUM(batch_count) total_count
                            from Stage1
                            group by StationId, Year_Rec, Month_Rec, Variable) temp) AS SourceTable
                        PIVOT
                        (
                          AVG(Average)
                          FOR Variable IN ([PRCP], [TMAX], [TMIN],[SNWD])
                        ) AS PivotTable) as final""")
    cursor.commit()
    cursor.close()


if __name__ == '__main__':
    spark, sc = init_spark('ofek')
    kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
    print("Kafka started streaming:")
    kafka_stream()
    print("\tKafka data streamed")
    print("Grouping and pivoting")
    groupby_batchid()
    print("\tGrouped by batchID, also preformed PIVOT")
    print("Adding Geospatial data to sql")
    # add_additional_data()
    print("\tGeospatial data now in sql")
    print("")
    print("done")

# com.microsoft.sqlserver.jdbc.spark
# https://noamcluster1hdistorage.blob.core.windows.net//ex2/ghcnd-stations.txt

import findspark

findspark.init()
import pandas as pd
import pyspark.sql
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
from time import sleep
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# environment data
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4," \
                             "com.microsoft.azure:spark-mssql-connector:1.0.2 pyspark-shell"


def init_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
database_name = "ofek0glick"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "Stage2"
username = "ofek0glick"
password = "Qwerty12!"

spark, sc = init_spark('ofek')
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", 'Stage2_Roni') \
    .option("user", username) \
    .option("password", password).load()

stationData = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", 'StationData') \
    .option("user", username) \
    .option("password", password).load()
print("Stage 1")
jdbcDF.createOrReplaceTempView("Stage2_Roni")
print("Stage 2")
# jdbcDF = spark.sql(
#     "select *, lag(prcp,1) over (partition by StationId, Month_Rec order by Year_Rec) as PREV_PRCP from Stage2_Roni")

jdbcDF = spark.sql(
    """select roni1.*, roni2.PRCP ONE_YEAR_PRCP, roni3.PRCP TWO_YEAR_PRCP, roni4.PRCP FIVE_YEAR_PRCP
            from Stage2_Roni roni1, Stage2_Roni roni2, Stage2_Roni roni3, Stage2_Roni roni4
            where roni1.StationId = roni2.StationId and roni1.Year_Rec = roni2.Year_Rec+1 and
                roni1.Month_Rec = roni2.Month_Rec and roni2.PRCP is not null
                and roni1.StationId = roni3.StationId and roni1.Year_Rec = roni3.Year_Rec+2 and
                roni1.Month_Rec = roni3.Month_Rec and roni2.PRCP is not null
                and roni1.StationId = roni4.StationId and roni1.Year_Rec = roni4.Year_Rec+5 and
                roni1.Month_Rec = roni4.Month_Rec and roni4.PRCP is not null
""")

joined = jdbcDF.join(stationData, on='StationId').select('StationId', 'Year_Rec', 'Month_Rec', 'PRCP', 'TMAX', 'TMIN',
                                                         'ONE_YEAR_PRCP', 'TWO_YEAR_PRCP', 'FIVE_YEAR_PRCP',
                                                         'SNWD', 'Elevation')
joined.show()
print(joined.columns)
print("Dropping nulls")
joined = joined.na.drop("any")
assembler = VectorAssembler(
    inputCols=["TMAX", "TMIN", "SNWD", 'ONE_YEAR_PRCP', 'TWO_YEAR_PRCP', 'FIVE_YEAR_PRCP', ],
    outputCol="features")

output = assembler.transform(joined).drop("StationId", "Year_Rec", "Month_Rec", "TMAX", "TMIN", "SNWD", 'ONE_YEAR_PRCP',
                                          'TWO_YEAR_PRCP', 'FIVE_YEAR_PRCP', "Elevation").withColumn("label", F.col(
                                            "PRCP")).drop("PRCP")
output.show()
splits = output.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]
print("predicting?")
lr = LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
lrModel = lr.fit(train_df)
summary = lrModel.summary
print("Coefficients: " + str(lrModel.coefficients))
print("Intercept: " + str(lrModel.intercept))
summary.residuals.show()
print(f"Total Iterations: {summary.totalIterations}")
print(f"RMSE: {summary.rootMeanSquaredError}")
print(f"R-Squared: {summary.r2}")

lr_predictions = lrModel.transform(test_df)
lr_predictions.select("prediction", "label", "features").show(5)
from pyspark.ml.evaluation import RegressionEvaluator

lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                                   labelCol="label", metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))
test_result = lrModel.evaluate(test_df)
print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

#################################### Desicion Tree ########################################

from pyspark.ml.regression import DecisionTreeRegressor

dt = DecisionTreeRegressor(featuresCol='features', labelCol='label')
dt_model = dt.fit(train_df)
dt_predictions = dt_model.transform(test_df)
dt_evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = dt_evaluator.evaluate(dt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
print(dt_model.featureImportances)
#
# print("Writing to SQL")
# jdbcDF.write \
#     .format("jdbc") \
#     .mode("overwrite") \
#     .option("url", url) \
#     .option("dbtable", "LearningTable_Attempt2") \
#     .option("user", username) \
#     .option("password", password) \
#     .save()


# com.microsoft.sqlserver.jdbc.spark

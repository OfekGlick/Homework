import findspark
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import Row
import numpy as np

findspark.init()
from pyspark.sql import SparkSession


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def wrapper(cents):
    def cal_dist(point: list):
        dist = []
        for cent in cents:
            dist.append(sum([(xi - yi) ** 2 for xi, yi in zip(point, cent)]) ** 0.5)
        import numpy as np
        temp = np.array(dist)
        dist = min(temp)
        classification = np.argmin(temp)
        return dist, classification

    return cal_dist


def kmeans_fit(data: pyspark.sql.dataframe.DataFrame, k, max_iter, q, init):
    from time import time
    temp = []
    sstart = time()
    while max_iter >= 0:
        start = time()
        print(max_iter)
        calc = wrapper(init)
        init = np.array(init)
        data_k = data.rdd.map(lambda row: (row[0], row[1], row[2], *calc(row))).sortBy(lambda x: x[3]).sortBy(
            lambda x: x[4])
        new_rdd = []
        new_cents = []
        for i in range(k):
            new_rdd.append(data_k.filter(lambda x: x[4] == i).map(
                lambda row: np.array((row[0], row[1], row[2]))).zipWithIndex().filter(lambda x: x[1] != q).map(
                lambda x: x[0]))
            new_cents.append(new_rdd[i].mean())
        new_cents = np.stack(new_cents, axis=0)
        temp.append(new_cents)
        comparison = (new_cents == init).all()
        if comparison:
            print("Done")
            break
        init = new_cents
        max_iter -= 1
        print(time() - start)
    print("total time", time() - sstart)
    print(temp)


if __name__ == '__main__':
    spark, sc = init_spark('hw2')
    data = spark.read.parquet(r"random_data.parquet")
    k = 5
    max_iter = 30
    q = 10
    init = data.take(k)
    res = kmeans_fit(data, k, max_iter, q, init)

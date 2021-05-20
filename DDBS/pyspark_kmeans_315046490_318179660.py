import findspark
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, ArrayType, FloatType
from pyspark.sql import Row
import numpy as np
import operator

findspark.init()
from pyspark.sql import SparkSession


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def wrapper(cents):
    def cal_dist(point):
        point = list(point)
        dist = []
        for cent in cents:
            dist.append(sum([(xi - yi) ** 2 for xi, yi in zip(point, cent)]) ** 0.5)
        import numpy as np
        temp = np.array(dist)
        dist = min(temp)
        classification = np.argmin(temp)
        return float(dist), float(classification)

    return cal_dist


def kmeans_fit(data: pyspark.sql.dataframe.DataFrame, k, max_iter, q, init):
    temp = []
    while max_iter > 0:
        from time import time
        start = time()
        calc = wrapper(init)
        init = np.array(init)
        data_k = data.rdd.map(lambda row: (*list(row), *calc(row)))
        data_k = data_k.sortBy(lambda x: x[-2]).sortBy(lambda x: x[-1])
        new_rdd = []
        new_cents = []
        for i in range(k):
            new_rdd.append(data_k.filter(lambda x: x[-1] == i).map(
                lambda row: np.array(row)[:-2]).zipWithIndex().filter(lambda x: x[1] % q != 0).map(
                lambda x: x[0]))
            new_cents.append(new_rdd[i].mean())
        new_cents = np.stack(new_cents, axis=0)
        print("time:", time() - start)
        print(new_cents)
        temp.append(new_cents)
        if (new_cents == init).all():
            break
        init = new_cents
        max_iter -= 1
    to_return = sc.parallelize(temp[-1].tolist()).toDF()
    return to_return


if __name__ == '__main__':
    spark, sc = init_spark('hw2')
    data = spark.read.parquet(r"random_data.parquet")
    k = 3
    max_iter = 3
    q = 5
    init = data.take(k)
    res = kmeans_fit(data, k, max_iter, q, init)
    res.show()

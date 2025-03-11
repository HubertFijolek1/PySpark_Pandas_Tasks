from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf() \
    .set("spark.pyspark.python", r"C:\Users\huber\Desktop\Projects\PySpark_Pandas_Tasks\venv\Scripts\python.exe") \
    .set("spark.pyspark.driver.python", r"C:\Users\huber\Desktop\Projects\PySpark_Pandas_Tasks\venv\Scripts\python.exe")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
df.show()
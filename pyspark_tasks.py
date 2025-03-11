from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf() \
    .set("spark.pyspark.python", r"C:\Users\huber\Desktop\Projects\PySpark_Pandas_Tasks\venv\Scripts\python.exe") \
    .set("spark.pyspark.driver.python", r"C:\Users\huber\Desktop\Projects\PySpark_Pandas_Tasks\venv\Scripts\python.exe")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

'''
Q1 While ingesting customer data from an external source, you notice duplicate entries. How 
would you remove duplicates and retain only the latest entry based on a timestamp column
'''

data = [("101", "2023-12-01", 100), ["101", "2023-12-02", 150], 
        ["102", "2023-12-01", 200], ["102", "2023-12-02", 250]]
columns = ["customer_id", "timestamp", "amount"]
df = spark.createDataFrame(data, columns)

df.orderBy("timestamp").desc().dropDuplicates(["customer_id"]).show()

df.show()
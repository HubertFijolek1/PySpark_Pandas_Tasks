from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

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

df = df.withColumn('timestamp', col('timestamp').cast(DateType()))
df.printSchema()

# df.orderBy(df.timestamp.desc()).dropDuplicates(["customer_id"]).show() #solution 1
# df.orderBy(df['timestamp'], ascending = [0]).dropDuplicates(["customer_id"]).show() # solution 2
# df.orderBy(df['timestamp'], ascending = [False]).dropDuplicates(["customer_id"]).show() # solution 3

#solution4 - using subquery
df.createOrReplaceTempView("tableq1")
result = spark.sql("""
SELECT *
FROM (
    SELECT 
        customer_id,
        timestamp,
        amount,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY timestamp DESC) AS rank
    FROM tableq1
) sub
WHERE rank = 1
""")
# result.show()

#solution 5 - using CTE
result = spark.sql("""
WITH ranked AS (
    SELECT 
        customer_id,
        timestamp,
        amount,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY timestamp DESC) AS rank
    FROM tableq1
)
SELECT *
FROM ranked
WHERE rank = 1
""")
# result.show()

#solution 6  - Using PySpark's DataFrame API (groupBy and agg)
latest_timestamps_df = df.groupBy("customer_id").agg(
    {"timestamp": "max"}
).withColumnRenamed("max(timestamp)", "latest_timestamp")

final_df = df.join(
    latest_timestamps_df,
    (df.customer_id == latest_timestamps_df.customer_id) & 
    (df.timestamp == latest_timestamps_df["latest_timestamp"]),
    "inner"
).select(df["*"])

# final_df.show()

#solution 7 - Using Window Functions with PySpark DataFrame API
window_spec = Window.partitionBy("customer_id").orderBy(desc("timestamp"))

df_with_rank = df.withColumn("rank", row_number().over(window_spec))
latest_records_df = df_with_rank.filter(col("rank") == 1).drop("rank")
# latest_records_df.show()
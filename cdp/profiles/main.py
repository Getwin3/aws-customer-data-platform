from cdp.utils.spark_session import get_spark
from pyspark.sql.functions import count, max

spark = get_spark("profiles")

profiles = spark.read.parquet("s3://cdp/staging/customers")
transactions = spark.read.parquet("s3://datalake/transactions")

df = profiles.join(transactions, "customer_id", "left")

df = df.groupBy("customer_id").agg(
    count("transaction_id").alias("total_transactions"),
    max("transaction_date").alias("last_purchase")
)

df.write.mode("overwrite").parquet("s3://cdp/profiles")
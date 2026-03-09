from cdp.utils.spark_session import get_spark
from source_normalization import normalize_customer_schema

spark = get_spark("ingestion")

df = spark.read.parquet("s3://datalake/raw/customers")

df = normalize_customer_schema(df)

df.write.mode("overwrite").parquet("s3://cdp/staging/customers")
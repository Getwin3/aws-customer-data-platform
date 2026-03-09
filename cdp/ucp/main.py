from cdp.utils.spark_session import get_spark
from golden_record import build_golden_record

spark = get_spark("ucp")

profiles = spark.read.parquet("s3://cdp/profiles")
clusters = spark.read.parquet("s3://cdp/dedupe")

ucp = build_golden_record(profiles, clusters)

ucp.write.mode("overwrite").parquet("s3://cdp/ucp")
from cdp.utils.spark_session import get_spark
from fuzzy_matching import fuzzy_match

spark = get_spark("dedupe")

profiles = spark.read.parquet("s3://cdp/profiles")

matches = fuzzy_match(profiles)

matches.write.mode("overwrite").parquet("s3://cdp/dedupe")
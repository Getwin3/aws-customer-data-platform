from pyspark.sql import SparkSession

def get_spark(app):

    spark = (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
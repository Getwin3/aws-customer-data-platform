from pyspark.sql.functions import col

def build_clusters(matches):

    clusters = matches.select(
        col("a.customer_id").alias("id1"),
        col("b.customer_id").alias("id2")
    )

    return clusters.dropDuplicates()
from pyspark.sql.functions import coalesce, col

def build_golden_record(profiles, clusters):

    df = profiles.join(
        clusters,
        profiles.customer_id == clusters.id1,
        "left"
    )

    df = df.select(
        coalesce(col("id1"), col("customer_id")).alias("master_customer_id"),
        col("email"),
        col("phone"),
        col("last_purchase"),
        col("total_transactions")
    )

    return df
from pyspark.sql.functions import max

def apply_survivorship(df):

    return df.groupBy("master_customer_id").agg(
        max("last_purchase").alias("last_purchase"),
        max("total_transactions").alias("total_transactions")
    )
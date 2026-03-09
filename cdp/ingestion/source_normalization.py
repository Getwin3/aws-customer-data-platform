from pyspark.sql.functions import col, trim, lower

def normalize_customer_schema(df):

    df = df.withColumn("email", lower(trim(col("email"))))

    df = df.withColumn(
        "phone",
        trim(col("phone"))
    )

    return df
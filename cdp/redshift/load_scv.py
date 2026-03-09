def load_to_redshift(df):

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:redshift://cluster:5439/dev") \
        .option("dbtable", "single_customer_view") \
        .option("user", "admin") \
        .option("password", "password") \
        .mode("append") \
        .save()
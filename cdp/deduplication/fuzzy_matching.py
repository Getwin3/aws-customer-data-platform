from pyspark.sql.functions import levenshtein, col

def fuzzy_match(df):

    pairs = df.alias("a").join(
        df.alias("b"),
        col("a.email") == col("b.email")
    )

    pairs = pairs.withColumn(
        "name_distance",
        levenshtein(col("a.first_name"), col("b.first_name"))
    )

    return pairs.filter(col("name_distance") < 3)
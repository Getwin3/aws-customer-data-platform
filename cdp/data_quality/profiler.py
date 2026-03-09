def profile(df):

    rows = df.count()

    null_email = df.filter("email IS NULL").count()

    return {
        "rows": rows,
        "missing_email": null_email
    }
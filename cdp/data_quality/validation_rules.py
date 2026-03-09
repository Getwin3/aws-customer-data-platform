def validate_customer(df):

    if df.filter("customer_id IS NULL").count() > 0:

        raise Exception("Invalid customer data")
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def mask_email(email):

    if email is None:
        return None

    name, domain = email.split("@")

    return name[:2] + "***@" + domain

mask_email_udf = udf(mask_email, StringType())
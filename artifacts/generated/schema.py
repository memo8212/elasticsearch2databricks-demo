"""StructTypes derived from Elasticsearch index mappings."""
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               IntegerType, DoubleType, BooleanType, TimestampType,
                               ArrayType)

DEPT_TXN_SCHEMA = StructType([
    StructField("doc_id", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("account_no", StringType(), True),
    StructField("holder_name", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("base_amount", DoubleType(), True),
    StructField("ccy", StringType(), True),
    StructField("txn_date", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("narrative", StringType(), True),
    StructField("tags", StringType(), True),
])

STATEMENTS_SCHEMA = StructType([
    StructField("statement_id", StringType(), True),
    StructField("account_no", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("period", StringType(), True),
    StructField("body", StringType(), True),
    StructField("attachment", StructType([
        StructField("name", StringType(), True),
        StructField("size", LongType(), True),
        StructField("pages", IntegerType(), True)
    ]), True),
    StructField("generated_at", TimestampType(), True),
])

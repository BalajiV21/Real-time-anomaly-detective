"""
Spark StructType schemas for messages on each Kafka topic.
"""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TRADE_SCHEMA = StructType([
    StructField("symbol",          StringType(),            True),
    StructField("price",           DoubleType(),            True),
    StructField("volume",          DoubleType(),            True),
    StructField("trade_timestamp", StringType(),            True),
    StructField("conditions",      ArrayType(StringType()), True),
])

QUOTE_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("c",      DoubleType(), True),
    StructField("d",      DoubleType(), True),
    StructField("dp",     DoubleType(), True),
    StructField("h",      DoubleType(), True),
    StructField("l",      DoubleType(), True),
    StructField("o",      DoubleType(), True),
    StructField("pc",     DoubleType(), True),
])

NEWS_SCHEMA = StructType([
    StructField("id",       LongType(),   True),
    StructField("category", StringType(), True),
    StructField("datetime", LongType(),   True),
    StructField("headline", StringType(), True),
    StructField("related",  StringType(), True),
    StructField("source",   StringType(), True),
    StructField("summary",  StringType(), True),
    StructField("url",      StringType(), True),
])

SENTIMENT_SCHEMA = StructType([
    StructField("symbol",                      StringType(), True),
    StructField("companyNewsScore",            DoubleType(), True),
    StructField("sectorAverageBullishPercent", DoubleType(), True),
    StructField("sectorAverageBearishPercent", DoubleType(), True),
])

ANOMALY_SCHEMA = StructType([
    StructField("symbol",           StringType(), True),
    StructField("anomaly_type",     StringType(), True),
    StructField("severity",         StringType(), True),
    StructField("detection_method", StringType(), True),
    StructField("anomaly_score",    DoubleType(), True),
    StructField("price",            DoubleType(), True),
    StructField("volume",           DoubleType(), True),
    StructField("detected_at",      StringType(), True),
])

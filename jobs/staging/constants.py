"""
Constants
"""
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, LongType

MOVIES = "movies"
RATINGS = "ratings"
LINKS = "links"
TAGS = "tags"

RATINGS_SCHEMA = StructType() \
    .add("userId", IntegerType(), True) \
    .add("movieId", IntegerType(), True) \
    .add("rating", DoubleType(), True) \
    .add("timestamp", LongType(), True)

RATINGS_SCHEMA_WITH_PARTITION = StructType() \
    .add("userId", IntegerType(), True) \
    .add("movieId", IntegerType(), True) \
    .add("rating", DoubleType(), True) \
    .add("timestamp", LongType(), True) \
    .add("timestamp_datetime", StringType(), True)

MOVIES_SCHEMA = StructType() \
    .add("movieId", IntegerType(), True) \
    .add("title", StringType(), True) \
    .add("genres", StringType(), True)

TAGS_SCHEMA = StructType() \
    .add("userId", IntegerType(), True) \
    .add("movieId", IntegerType(), True) \
    .add("tag", StringType(), True) \
    .add("timestamp", LongType(), True)

TOP_K_MOVIES_SCHEMA = StructType() \
    .add("movieId", IntegerType(), True) \
    .add("avg_rating", DoubleType(), True)

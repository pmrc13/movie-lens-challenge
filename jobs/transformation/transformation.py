""" Transformation layer.
    :return: None
    """
from os import getcwd
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, split, mean, count
from delta import configure_spark_with_delta_pip


def main():
    """ Runs the transformation layer's job.
    :return: None
    """
    builder = SparkSession \
        .builder \
        .master("local[1]") \
        .appName("Transformation")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    cwd = getcwd()

    data_directory = f"{cwd}/jobs/staging/tables"

    transform_movies(spark,
                     f"{data_directory}/movies-delta-table",
                     f"{cwd}/jobs/transformation/tables/movies-split-delta-table")

    k = 10
    find_top_k_films_by_avg_rating(
        spark,
        f"{data_directory}/ratings-delta-table",
        f"{cwd}/jobs/transformation/top_{str(k)}_movies/top_{str(k)}_movies.csv",
        k
    )


def transform_movies(spark, data_directory, save_path):
    """ Performs the first task of the transformation layer.
    Explodes the pipe-separated "genres" column to separate rows.
    :return: Dataframe
    """
    movies_df = spark.read.format("delta").load(data_directory)
    movies_genres_split_df = movies_df.withColumn("genres", explode(split(col("genres"), '\\|')))

    movies_genres_split_df.write \
        .mode('overwrite') \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .save(save_path)

    return movies_genres_split_df


def find_top_k_films_by_avg_rating(spark, data_directory, save_path, k=10):
    """ Performs the last task of the transformation layer.
    Finds the top k (default is 10) films having a higher
    average rating score.
    :return: Dataframe
    """
    ratings_df = spark \
        .read \
        .format("delta") \
        .load(data_directory)

    movie_avg_ratings_df = ratings_df \
        .groupBy("movieId") \
        .agg(mean('rating').alias("avg_rating"), count('rating').alias("count_rating")) \
        .filter(col("count_rating") >= 5)

    # the average rating score ties are handled by adding additional grouping conditions
    # i.e. if two movies have the same average score, order by number of ratings given by users.
    # if the movies have the same number of ratings as well, order by movieId
    ordered_movie_avg_ratings_df = movie_avg_ratings_df \
        .orderBy(col("avg_rating").desc(), col("count_rating").desc(), col("movieId").asc()) \
        .limit(k) \
        .select(col("movieId"), col("avg_rating"))

    ordered_movie_avg_ratings_df \
        .coalesce(1) \
        .toPandas() \
        .to_csv(save_path, index=False)

    return ordered_movie_avg_ratings_df


if __name__ == '__main__':
    main()

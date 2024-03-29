""" Staging layer.
    :return: None
    """
from os import listdir, getcwd
from os.path import isfile, join
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col
from delta import configure_spark_with_delta_pip
from jobs.staging import constants


def main():
    """ Runs the staging layer's job.
    :return: None
    """
    builder = SparkSession \
        .builder \
        .master("local[1]") \
        .appName("Staging")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    cwd = getcwd()

    data_directory = f"{cwd}/datasets/"

    load_ratings_data(spark, data_directory, f"{cwd}/jobs/staging/tables/ratings-delta-table")
    load_movies_data(spark, data_directory, f"{cwd}/jobs/staging/tables/movies-delta-table")
    load_tags_data(spark, data_directory, f"{cwd}/jobs/staging/tables/tags-delta-table")


def load_ratings_data(spark, data_directory, save_path):
    """ Load data from the CSV files related to movie ratings and
    save it in partitioned a delta lake table.

    Keyword arguments:
    spark -- the spark session
    data_directory -- CSV input file location
    save_path -- resulting delta lake table location

    :return: Dataframe
    """
    ratings_df = process_data_files(spark,
                                    data_directory,
                                    constants.RATINGS,
                                    constants.RATINGS_SCHEMA)

    ratings_df_with_datetime = ratings_df\
        .withColumn("timestamp_datetime", from_unixtime(col("timestamp"), "MM-yyyy"))

    ratings_df_with_datetime.write \
        .mode('overwrite') \
        .format("delta") \
        .partitionBy("timestamp_datetime") \
        .option("overwriteSchema", "true") \
        .save(save_path)

    return ratings_df_with_datetime


def load_movies_data(spark, data_directory, save_path):
    """ Load data from the CSV files related to movies and save it in a delta lake table.

    Keyword arguments:
    spark -- the spark session
    data_directory -- CSV input file location
    save_path -- resulting delta lake table location

    :return: Dataframe
    """
    movies_df = process_data_files(spark, data_directory, constants.MOVIES, constants.MOVIES_SCHEMA)
    movies_df.write \
        .mode('overwrite') \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .save(save_path)

    return movies_df


def load_tags_data(spark, data_directory, save_path):
    """ Load data from the CSV files related to movie tags and save it in a delta lake table.

    Keyword arguments:
    spark -- the spark session
    data_directory -- CSV input file location
    save_path -- resulting delta lake table location

    :return: Dataframe
    """
    tags_df = process_data_files(spark, data_directory, constants.TAGS, constants.TAGS_SCHEMA)
    tags_df.write \
        .mode('overwrite') \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .save(save_path)

    return tags_df


def get_dataset_files(data_directory, dataset_name):
    """ Helper function that obtains all the CSV files for a certain dataset

    Keyword arguments:
    data_directory -- CSV input files location
    dataset_name -- the desired dataset: ratings, movies, or tags

    :return: List[String] sorted list of files
    """
    dataset_files = [f"{data_directory}/{f}" for f in listdir(data_directory) if
                     isfile(join(data_directory, f)) and dataset_name in f]
    return sorted(dataset_files)


def process_data_files(spark, data_directory, dataset, schema):
    """ Helper function that reads all CSV files, for the respective dataset.

    Keyword arguments:
    spark -- the spark session
    data_directory -- CSV input file location
    dataset -- the desired dataset: ratings, movies, or tags
    schema -- the desired dataset schema

    :return: Dataframe
    """
    files = get_dataset_files(data_directory, dataset)
    number_of_files = len(files)

    # read the original CSV file
    current_data_df = spark.read.option("header", True).schema(schema).csv(files[0])

    if number_of_files > 1:
        for idx in range(1, number_of_files):
            file = files[idx]
            new_df = spark.read.option("header", True).schema(schema).csv(file)
            if dataset == constants.RATINGS:
                updated_df = new_df.subtract(current_data_df).unionAll(
                    current_data_df.join(new_df, ["userId", "movieId"], "left_anti"))
            else:
                updated_df = current_data_df.union(new_df)
            current_data_df = updated_df

    return current_data_df


if __name__ == '__main__':
    main()

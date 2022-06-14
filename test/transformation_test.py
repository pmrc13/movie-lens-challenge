import unittest
import pyspark
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from jobs.staging import constants
from jobs.staging.staging import load_ratings_data
from jobs.transformation.transformation import *


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        builder = (SparkSession
                   .builder
                   .master("local[1]")
                   .appName("TransformationTest"))

        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()

        cwd = getcwd()
        print(cwd)

        cls.data_directory = f"{cwd}/test/resources"

        cls.load_test_ratings_top_k_file(cls)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_load_movies_data(self):
        expected_df = self.spark.createDataFrame(
            data=[
                [1, "Toy Story (1995)", "Adventure"],
                [1, "Toy Story (1995)", "Animation"],
                [1, "Toy Story (1995)", "Children"],
                [1, "Toy Story (1995)", "Comedy"],
                [1, "Toy Story (1995)", "Fantasy"],
                [2, "Jumanji (1995)", "Adventure"],
                [2, "Jumanji (1995)", "Children"],
                [2, "Jumanji (1995)", "Fantasy"],
                [3, "Grumpier Old Men (1995)", "Comedy"],
                [3, "Grumpier Old Men (1995)", "Romance"],
                [4, "Waiting to Exhale (1995)", "Comedy"],
                [4, "Waiting to Exhale (1995)", "Drama"],
                [4, "Waiting to Exhale (1995)", "Romance"],
                [5, "Father of the Bride Part II (1995)", "Comedy"]
            ],
            schema=constants.MOVIES_SCHEMA)

        actual_df = transform_movies(self.spark,
                                     f"{self.data_directory}/movies-delta-table",
                                     f"{self.data_directory}/movies-split-delta-table")

        expected_df.show()
        actual_df.show()

        self.assertEqual(expected_df.collect(), actual_df.collect())

    def test_find_top_k_films_by_avg_rating(self):
        expected_df = self.spark.createDataFrame(
            data=[
                [1, 4.75],
                [8, 4.185714285714286],
                [9, 4.0125],
                [5, 4.0],
                [4, 4.0]
            ],
            schema=constants.TOP_K_MOVIES_SCHEMA)

        k = 5
        actual_df = find_top_k_films_by_avg_rating(self.spark,
                                                   f"{self.data_directory}/ratings-top-k-movies-delta-table",
                                                   f"{self.data_directory}/top_{str(k)}_movies/top_{str(k)}_movies.csv",
                                                   k)

        expected_df.show()
        actual_df.show()

        self.assertEqual(expected_df.collect(), actual_df.collect())

    def load_test_ratings_top_k_file(self):
        ratings_df = self.spark.read \
            .option("header", True) \
            .schema(constants.RATINGS_SCHEMA) \
            .csv(f"{self.data_directory}/top_k_rts_input.csv")

        ratings_df_with_datetime = ratings_df.withColumn("timestamp_datetime",
                                                         from_unixtime(col("timestamp"), "MM-yyyy"))

        ratings_df_with_datetime.write \
            .mode('overwrite') \
            .format("delta") \
            .partitionBy("timestamp_datetime") \
            .option("overwriteSchema", "true") \
            .save(f"{self.data_directory}/ratings-top-k-movies-delta-table")


if __name__ == '__main__':
    unittest.main()

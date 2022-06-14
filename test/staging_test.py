import unittest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from jobs.staging.staging import *


class StagingTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        builder = (SparkSession
                   .builder
                   .master("local[1]")
                   .appName("StagingTest"))

        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()

        cwd = getcwd()

        cls.data_directory = f"{cwd}/test/resources"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_load_ratings_data(self):
        expected_df = self.spark.createDataFrame(data=[[1, 1, 1.0, 1654889661, "06-2022"],
                                                       [1, 3, 1.0, 1654889665, "06-2022"],
                                                       [1, 6, 1.0, 1654889668, "06-2022"],
                                                       [5, 6, 4.0, 1654889676, "06-2022"],
                                                       [2, 1, 3.0, 1654889669, "06-2022"],
                                                       [2, 3, 4.5, 1654889672, "06-2022"],
                                                       [3, 6, 4.0, 1654889676, "06-2022"]],
                                                 schema=constants.RATINGS_SCHEMA_WITH_PARTITION)

        actual_df = load_ratings_data(self.spark, self.data_directory, f"{self.data_directory}/ratings-delta-table")

        self.assertEqual(expected_df.collect(), actual_df.collect())

    def test_load_ratings_data_first_csv_only(self):
        expected_df = self.spark.createDataFrame(data=[[1, 1, 4.0, 1654889661, "06-2022"],
                                                       [1, 3, 2.0, 1654889665, "06-2022"],
                                                       [1, 6, 4.0, 1654889668, "06-2022"],
                                                       [2, 1, 3.0, 1654889669, "06-2022"],
                                                       [2, 3, 4.5, 1654889672, "06-2022"],
                                                       [3, 6, 4.0, 1654889676, "06-2022"]],
                                                 schema=constants.RATINGS_SCHEMA_WITH_PARTITION)

        # data_directory = "/Users/pedro.cheira/Documents/learning/asos-movie-lens/tests/resources/"

        actual_df = load_ratings_data(self.spark, self.data_directory, f"{self.data_directory}/ratings-delta-table")

        self.assertNotEqual(expected_df.collect(), actual_df.collect())

    def test_load_ratings_data_table_created(self):
        expected_df = self.spark.createDataFrame(data=[[1, 1, 1.0, 1654889661, "06-2022"],
                                                       [1, 3, 1.0, 1654889665, "06-2022"],
                                                       [1, 6, 1.0, 1654889668, "06-2022"],
                                                       [5, 6, 4.0, 1654889676, "06-2022"],
                                                       [2, 1, 3.0, 1654889669, "06-2022"],
                                                       [2, 3, 4.5, 1654889672, "06-2022"],
                                                       [3, 6, 4.0, 1654889676, "06-2022"]],
                                                 schema=constants.RATINGS_SCHEMA_WITH_PARTITION)

        save_path = f"{self.data_directory}/ratings-delta-table"

        _ = load_ratings_data(self.spark, self.data_directory, save_path)

        actual_df = self.spark.read.format("delta").load(save_path)

        self.assertEqual(sorted(expected_df.collect()), sorted(actual_df.collect()))

    def test_load_movies_data(self):
        expected_df = self.spark.createDataFrame(
            data=[
                [1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"],
                [2, "Jumanji (1995)", "Adventure|Children|Fantasy"],
                [3, "Grumpier Old Men (1995)", "Comedy|Romance"],
                [4, "Waiting to Exhale (1995)", "Comedy|Drama|Romance"],
                [5, "Father of the Bride Part II (1995)", "Comedy"]
            ],
            schema=constants.MOVIES_SCHEMA)

        actual_df = load_movies_data(self.spark, self.data_directory, f"{self.data_directory}/movies-delta-table")

        self.assertEqual(expected_df.collect(), actual_df.collect())

    def test_load_movies_data_table_created(self):
        expected_df = self.spark.createDataFrame(
            data=[
                [1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"],
                [2, "Jumanji (1995)", "Adventure|Children|Fantasy"],
                [3, "Grumpier Old Men (1995)", "Comedy|Romance"],
                [4, "Waiting to Exhale (1995)", "Comedy|Drama|Romance"],
                [5, "Father of the Bride Part II (1995)", "Comedy"]
            ],
            schema=constants.MOVIES_SCHEMA)

        save_path = f"{self.data_directory}/movies-delta-table"

        _ = load_movies_data(self.spark, self.data_directory, save_path)

        actual_df = self.spark.read.format("delta").load(save_path)

        self.assertEqual(sorted(expected_df.collect()), sorted(actual_df.collect()))

    def test_load_tags_data(self):
        expected_df = self.spark.createDataFrame(
            data=[
                [2, 60756, "funny", 1445714994],
                [2, 60756, "Highly quotable", 1445714996],
                [2, 60756, "will ferrell", 1445714992],
                [2, 89774, "Boxing story", 1445715207],
                [2, 89774, "MMA", 1445715200]
            ],
            schema=constants.TAGS_SCHEMA)

        actual_df = load_tags_data(self.spark, self.data_directory, f"{self.data_directory}/tags-delta-table")

        self.assertEqual(expected_df.collect(), actual_df.collect())

    def test_load_tags_data_table_created(self):
        expected_df = self.spark.createDataFrame(
            data=[
                [2, 60756, "funny", 1445714994],
                [2, 60756, "Highly quotable", 1445714996],
                [2, 60756, "will ferrell", 1445714992],
                [2, 89774, "Boxing story", 1445715207],
                [2, 89774, "MMA", 1445715200]
            ],
            schema=constants.TAGS_SCHEMA)

        save_path = f"{self.data_directory}/tags-delta-table"

        _ = load_tags_data(self.spark, self.data_directory, save_path)

        actual_df = self.spark.read.format("delta").load(save_path)

        self.assertEqual(sorted(expected_df.collect()), sorted(actual_df.collect()))

    def test_get_dataset_files(self):
        ratings_files = get_dataset_files(self.data_directory, constants.RATINGS)
        movies_files = get_dataset_files(self.data_directory, constants.MOVIES)
        tags_files = get_dataset_files(self.data_directory, constants.TAGS)

        self.assertEqual(len(ratings_files), 2)
        self.assertIn(f"{self.data_directory}/ratings.csv", ratings_files)
        self.assertIn(f"{self.data_directory}/ratings_2.csv", ratings_files)

        self.assertEqual(len(movies_files), 1)
        self.assertIn(f"{self.data_directory}/movies.csv", movies_files)

        self.assertEqual(len(tags_files), 1)
        self.assertIn(f"{self.data_directory}/tags.csv", tags_files)


if __name__ == '__main__':
    unittest.main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import unittest

from databricks_repo.assignment_1.source_to_bronze.utils import read_csv_with_custom_schema, convert_camel_to_snake_case, write_delta_table


#
# Import fun

class TestSparkFunctions(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("TestSparkFunctions") \
            .getOrCreate()



        # Define schema for sample data
        self.schema = "id INT, name STRING, age INT"

        # Define sample DataFrame
        self.df = self.spark.createDataFrame(self.sample_data, schema=self.schema)

    def tearDown(self):
        self.spark.stop()

    def test_read_csv_with_custom_schema(self):
        # Test read_csv_with_custom_schema function
        file_path = "dbfs:/FileStore/assignment/assignment_1/resource/Employee_Q1.csv"
        schema = "EmployeeID INT, EmployeeName STRING, Department STRING, Country STRING, Salary INT, Age INT"
        df = read_csv_with_custom_schema(file_path, schema)

        # Validate schema of DataFrame
        self.assertEqual(df.schema.simpleString(), self.schema)

    def test_convert_camel_to_snake_case(self):
        # Test convert_camel_to_snake_case function
        expected_df = self.df.select(col("id").alias("id"), col("name").alias("name"), col("age").alias("age"))
        actual_df = convert_camel_to_snake_case(self.df)

        # Validate column names
        self.assertEqual(expected_df.columns, actual_df.columns)


if __name__ == "__main__":
    unittest.main()

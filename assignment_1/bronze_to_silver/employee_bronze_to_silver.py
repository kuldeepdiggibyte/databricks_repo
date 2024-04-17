# Databricks notebook source
# MAGIC %run /Users/kuldeep.pralhadmanagoli@diggibyte.com/assignment_1/source_to_bronze/utils
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run /Users/kuldeep.pralhadmanagoli@diggibyte.com/assignment_1/source_to_bronze/employee_source_to_bronze

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS Employee_info")

# COMMAND ----------

employee_schema = "Employee_ID INT, Employee_Name STRING, Department STRING, Country STRING, Salary INT, Age INT, Load_Date DATE"

# COMMAND ----------

department_schema = "Department_ID STRING, Department_Name STRING, Load_Date DATE"
country_schema = "Country_Code STRING, Country_Name STRING, Load_Date DATE"

# COMMAND ----------

employee_df =spark.read.csv("dbfs:/FileStore/source_to_bronze/EmployeeQ1.csv", employee_schema)
department_df = spark.read.csv("dbfs:/FileStore/source_to_bronze/Department_Q1.csv", department_schema)
country_df = spark.read.csv("dbfs:/FileStore/source_to_bronze/Country_Q1.csv", country_schema)


# COMMAND ----------

employee_df = convert_camel_to_snake_case(employee_df)
department_df = convert_camel_to_snake_case(department_df)
country_df = convert_camel_to_snake_case(country_df)

# COMMAND ----------

from pyspark.sql.functions import current_date
employee_df = employee_df.withColumn("load_date", current_date())
department_df = department_df.withColumn("load_date", current_date())
country_df = country_df.withColumn("load_date", current_date())

# COMMAND ----------

display(employee_df)

# COMMAND ----------

display(department_df)

# COMMAND ----------

display(country_df)

# COMMAND ----------

write_delta_table(employee_df,"Employee_info","dim_employee", "EmployeeID", "/silver/Employee_info/dim_employee")

# COMMAND ----------


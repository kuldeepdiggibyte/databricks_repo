# Databricks notebook source
# MAGIC %run /Users/kuldeep.pralhadmanagoli@diggibyte.com/assignment_1/source_to_bronze/utils
# MAGIC

# COMMAND ----------

employee_schema = "EmployeeID INT, EmployeeName STRING, Department STRING, Country STRING, Salary INT, Age INT"
department_schema = "DepartmentID STRING, DepartmentName STRING"
country_schema = "CountryCode STRING, CountryName STRING"

# COMMAND ----------

employee_df = spark.read.csv("dbfs:/FileStore/assignment/assignment_1/resource/Employee_Q1.csv", header=True, schema=employee_schema)
employee_df.head()

# COMMAND ----------

department_df = spark.read.csv("dbfs:/FileStore/assignment/assignment_1/resource/Department_Q1.csv", header=True, schema=department_schema)
department_df.head()

# COMMAND ----------

country_df = spark.read.csv("dbfs:/FileStore/assignment/assignment_1/resource/Country_Q1.csv", header=True, schema=country_schema)
country_df.head()

# COMMAND ----------

employee_df.write.csv(path='dbfs:/FileStore/source_to_bronze/EmployeeQ1.csv')

# COMMAND ----------

data.write.csv(path='dbfs:/FileStore/source_to_bronze/Department_Q1.csv')

# COMMAND ----------

data.write.csv(path='dbfs:/FileStore/source_to_bronze/Country_Q1.csv')
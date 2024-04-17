# Databricks notebook source
# MAGIC %run ../source_to_bronze/utils

# COMMAND ----------

# MAGIC %run /Users/kuldeep.pralhadmanagoli@diggibyte.com/assignment_1/bronze_to_silver/employee_bronze_to_silver

# COMMAND ----------

employee_df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/employee_info.db/dim_employee")

# COMMAND ----------


from pyspark.sql.functions import avg,desc,count

# COMMAND ----------

salary_by_department = employee_df.groupBy("department").agg(avg("salary").alias("avg_salary")).orderBy(desc("avg_salary"))

# COMMAND ----------

employee_count = employee_df.groupBy("department", "country").agg(count("employee_id").alias("employee_count"))
display(employee_count)

# COMMAND ----------

avg_age_by_department = employee_df.groupBy('department').agg(avg("age").alias('avg_age'))
display(avg_age_by_department)

# COMMAND ----------

employee_df.write.format("parquet").mode("overwrite").option("replaceWhere","load_date = '2024-04-16'").save("/FileStore/assignments/assignment_1/gold/employee/table_name")

# COMMAND ----------


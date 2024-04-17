
# Databricks Assignment

## Question 1:

### Folder Structure and Notebook Descriptions

For this question, we have organized the project into three folders: source_to_bronze, bronze_to_silver, and silver_to_gold. Each folder contains specific notebooks as described below:

1. **source_to_bronze:**
    - **utils.ipynb:** This notebook holds all common functions utilized across the project.
    - **employee_source_to_bronze.ipynb:** This is the driver notebook responsible for reading the raw data, applying necessary transformations, and writing the processed data to the bronze layer.

2. **bronze_to_silver:**
    - **employee_bronze_to_silver.ipynb:** This notebook reads data from the bronze layer, applies further transformations including converting camel case to snake case, adds load_date column, and writes the transformed data to the silver layer.

3. **silver_to_gold:**
    - **employee_silver_to_gold.ipynb:** In this notebook, data is read from the silver layer, various analyses are performed based on specific requirements, load_date column is added, and the final DataFrame is written to the gold layer.

### Instructions for Each Notebook

- **employee_source_to_bronze.ipynb:**
    1. Reads the raw datasets as DataFrames.
    2. Calls functions from `utils.ipynb`.
    3. Writes the transformed DataFrames to DBFS at `/source_to_bronze/` as CSV files.

- **employee_bronze_to_silver.ipynb:**
    4. Reads the data from DBFS located in `/source_to_bronze/`.
    5. Applies custom schema and transformations including camel case to snake case conversion.
    6. Adds the `load_date` column with the current date.
    7. Writes the DataFrame as a delta table to the location `/silver/Employee_info/dim_employee`.

- **employee_silver_to_gold.ipynb:**
    8. Reads the delta table from the silver layer.
    9. Performs various analyses and transformations based on the given requirements.
    10. Adds the `at_load_date` column.
    11. Writes the DataFrame to DBFS at `/gold/employee/fact_employee` with overwrite and replace conditions on `at_load_date`.

## Question 2:

### API Data Ingestion and Transformation

- Fetches data from the provided API by passing the parameter as a page and retrieves the data until the response is empty.
- Reads the data frame with a custom schema.
- Flattens the dataframe.
- Derives a new column named `site_address` from the `email` column with the value `reqres.in`.
- Adds `load_date` with the current date.
- Writes the data frame to a location in DBFS as `/site_info/person_info` with delta format and overwrite mode.

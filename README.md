# databricks_repo

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Project README</title>
</head>
<body>
    <h1>Project README</h1>

    <h2>Folder Structure and Notebook Descriptions</h2>
    <p>For this project, we have created three folders: source_to_bronze, bronze_to_silver, and silver_to_gold. Each folder contains specific notebooks as follows:</p>
    <ul>
        <li><strong>source_to_bronze:</strong>
            <ul>
                <li><strong>utils.ipynb:</strong> This notebook contains common functions used throughout the project.</li>
                <li><strong>employee_source_to_bronze.ipynb:</strong> This is the driver notebook responsible for reading the raw data and writing to the bronze layer.</li>
            </ul>
        </li>
        <li><strong>bronze_to_silver:</strong>
            <ul>
                <li><strong>employee_bronze_to_silver.ipynb:</strong> This notebook reads data from the bronze layer, applies transformations, and writes to the silver layer.</li>
            </ul>
        </li>
        <li><strong>silver_to_gold:</strong>
            <ul>
                <li><strong>employee_silver_to_gold.ipynb:</strong> This notebook reads data from the silver layer, performs further processing, and writes to the gold layer.</li>
            </ul>
        </li>
    </ul>

    <h2>Instructions for Each Notebook</h2>

    <h3>1. employee_source_to_bronze.ipynb:</h3>
    <ol>
        <li>Read the raw datasets as DataFrames.</li>
        <li>Call functions from <strong>utils.ipynb</strong>.</li>
        <li>Write the transformed DataFrames to DBFS at <code>/source_to_bronze/</code>.</li>
    </ol>

    <h3>2. employee_bronze_to_silver.ipynb:</h3>
    <ol start="4">
        <li>Read the data from DBFS located in <code>/source_to_bronze/</code>.</li>
        <li>Apply custom schema and transformations.</li>
        <li>Add the <code>load_date</code> column.</li>
        <li>Write the DataFrame as a delta table to <code>/silver/Employee_info/dim_employee</code>.</li>
    </ol>

    <h3>3. employee_silver_to_gold.ipynb:</h3>
    <ol start="8">
        <li>Read the delta table from the silver layer.</li>
        <li>Perform various analyses and transformations based on the given requirements.</li>
        <li>Add the <code>at_load_date</code> column.</li>
        <li>Write the DataFrame to DBFS at <code>/gold/employee/fact_employee</code> with overwrite and replace conditions.</li>
    </ol>

    <h2>Additional Requirements</h2>
    <p>Ensure that each notebook calls the <strong>utils.ipynb</strong> notebook for common functions and maintains proper documentation.</p>

</body>
</html>

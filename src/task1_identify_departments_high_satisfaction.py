from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round as spark_round

def initialize_spark(app_name="Task1_Identify_Departments"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the employee data from a CSV file into a Spark DataFrame.

    Parameters:
        spark (SparkSession): The SparkSession object.
        file_path (str): Path to the employee_data.csv file.

    Returns:
        DataFrame: Spark DataFrame containing employee data.
    """
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_departments_high_satisfaction(df):
    """
    Identify departments with more than 50% of employees having a Satisfaction Rating > 4 and Engagement Level 'High'.

    Parameters:
        df (DataFrame): Spark DataFrame containing employee data.

    Returns:
        DataFrame: DataFrame containing departments meeting the criteria with their respective percentages.
    """
    # TODO: Implement Task 1
    # Steps:
    # 1. Filter employees with SatisfactionRating > 4 and EngagementLevel == 'High'.
    # 2. Calculate the percentage of such employees within each department.
    # 3. Identify departments where this percentage exceeds 50%.
    # 4. Return the result DataFrame.

    #pass  # Remove this line after implementing the function
    # Filter employees with SatisfactionRating > 4 and EngagementLevel == 'High'
    high_satisfaction_df = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High"))

    # Count total employees per department
    total_employees_df = df.groupBy("Department").agg(count("*").alias("TotalEmployees"))

    # Count employees who meet the criteria per department
    high_satisfaction_count_df = high_satisfaction_df.groupBy("Department").agg(count("*").alias("SatisfiedEngaged"))

    # Calculate the percentage
    result_df = high_satisfaction_count_df.join(total_employees_df, "Department").withColumn(
        "Percentage", spark_round((col("SatisfiedEngaged") / col("TotalEmployees")) * 100, 2)
    ).filter(col("Percentage") > 5)
    
    filtered_df = result_df.select("Department", "Percentage")

    # Select only required columns
    return filtered_df  

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.

    Parameters:
        result_df (DataFrame): Spark DataFrame containing the result.
        output_path (str): Path to save the output CSV file.

    Returns:
        None
    """

    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

def main():
    """
    Main function to execute Task 1.
    """
    # Initialize Spark
    spark = initialize_spark()
    
    # Define file paths
    #input_file = "/workspaces/Employee_Engagement_Analysis_Spark/input/employee_data.csv"
    input_file = "input/employee_data.csv"
    output_file = "outputs/task1"
    
    # Load data
    df = load_data(spark, input_file)
    
    # Perform Task 1
    result_df = identify_departments_high_satisfaction(df)
    
    # Write the result to CSV
    write_output(result_df, output_file)
    
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()

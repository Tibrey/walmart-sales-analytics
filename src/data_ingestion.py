"""
This module handles loading of data from various sources into PySpark DataFrames
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, DateType, BooleanType
import logging

logger = logging.getLogger(__name__)

def get_walmart_schema():
    """
    Define explicit schema for walmart sales data

    Returns:
        StructType: Schema definition for the dataset
    """
    return StructType ([
        StructField("Store", IntegerType(), True),
        StructField("Date", StringType(), True),
        StructField("Weekly_Sales", FloatType(), True),
        StructField("Holiday_Flag", IntegerType(), True),
        StructField("Temperature", FloatType(), True),
        StructField("Fuel_Price", FloatType(), True),
        StructField("CPI", FloatType(), True),
        StructField("Unemployment", FloatType(), True),
    ])

def load_csv_data(spark, file_path, schema=None, header=True, infer_schema=False):
    """
    Loads CSV data into Spark Dataframe with specified schema

    Args:
        spark (SparkSession): Active Spark session
        file_path (str): Path to CSV file
        schema (StructType): Optional explicit schema
        header (bool): Whether CSV has header row
        infer_schema (bool): Whether to infer schema (use False for performance)

    Returns:
        DataFrame: Loaded Spark DataFrame
    """
    try:
        logger.info(f"Loading data from: {file_path}")

        if schema:
            df = spark.read \
                .format("csv") \
                .option("header",header) \
                .schema(schema) \
                .load(file_path)
        else:
            df = spark.read \
                .format("csv") \
                .option("header", header) \
                .option("inferSchema",infer_schema) \
                .load(file_path)
        
        record_count = df.count()
        logger.info(f"Successfully loaded {record_count} records")
        logger.info(f"Schema: {df.schema}")

        return df
    
    except Exception as e:
        logger.error(f"Failed to load data : {str(e)}")
        raise

def load_walmart_sales_data(spark, file_path):
    """
    Load Walmart sales data with predefined schema
    
    Args:
        spark (SparkSession): Active spark session
        file_path (str): Path to walmart sales csv file

    Returns:
        DataFrame: Loaded Walmart Sales DataFrame
    """

    schema = get_walmart_schema()
    df = load_csv_data(spark, file_path, schema=schema)

    # Display basic info
    # Display basic info
    print("\n=== Data Loading Summary ===")
    print(f"Total Records: {df.count()}")
    print(f"Total Columns: {len(df.columns)}")
    print(f"\nColumn Names: {df.columns}")

    return df

def display_data_sample(df,n=5):
    """
    Display sample records from DataFrame

    Args:
        df (DataFrame): Spark DataFrame
        n (int): Number of records to display
    """
    print(f"\n=== Sample Data (First {n} Records) ===")
    df.show(n, truncate=False)


def get_data_statistics(df):
    """
    Get basic statistics about the dataset

    Args:
        df (DataFrame): Spark DataFrame
    
    Returns:
        dict: Dictionary containing dataset statistics
    """

    stats = {
        "total_records": df.count(),
        "total_columns": len(df.columns),
        "columns":df.columns,
        "null_counts":{col:df.filter(df[col].isNull()).count() for col in df.columns}
    }

    return stats

def save_dataframe(df, output_path, format="parquet", mode="overwrite", partition_by=None):
    """
    Save DataFrame to disk in specified format
    
    Args:
        df (DataFrame): Spark DataFrame to save
        output_path (str): Output file path
        format (str): Output format (parquet, csv, json)
        mode (str): Save mode (overwrite, append, ignore, error)
        partition_by (list): Optional list of columns to partition by
    """

    try:
        logger.info(f"Saving DataFrame to {output_path} in {format} format")

        writer = df.write.mode(mode).format(format)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(output_path)
        logger.info("DataFrame saved successfully")
    
    except Exception as e:
        logger.error(f"Failed to save DataFrame: {str(e)}")
        raise

# Example usage
# if __name__ == "__main__":
#     from spark_session import create_spark_session,stop_spark_session

#     spark = create_spark_session()

#     data_path = "../data/raw/Walmart_Sales.csv"

#     try:
#         df = load_walmart_sales_data(spark,data_path)
#         display_data_sample(df)

#         stats = get_data_statistics(df)
#         print(f"\n=== Dataset Statistics ===")
#         print(f"Total Records: {stats['total_records']}")
#         print(f"Total Columns: {stats['total_columns']}")
#         print(f"\nNull Counts:")
#         for col, count in stats['null_counts'].items():
#             print(f"  {col}: {count}")
        
#     finally:
#         stop_spark_session()
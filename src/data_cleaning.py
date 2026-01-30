"""
Data Cleaning Module
Handles data quality checks, missing value treatment, and data validation
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, count, avg, stddev, percentile_approx
from pyspark.sql.functions import to_date, year, month, dayofmonth, dayofweek
import logging

logger = logging.getLogger(__name__)


def check_null_values(df):
    """
    Check for null/missing values in all columns
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        DataFrame: Summary of null counts per column
    """
    null_counts = df.select([
        count(when(col(c).isNull() | isnan(c), c)).alias(c) 
        for c in df.columns
    ])
    
    print("\n=== Null Value Analysis ===")
    null_counts.show()
    
    return null_counts


def handle_missing_values(df, strategy="mean", columns=None):
    """
    Handle missing values using specified strategy
    
    Args:
        df (DataFrame): Input DataFrame
        strategy (str): Strategy to handle missing values (mean, median, mode, drop, fill)
        columns (list): List of columns to apply strategy to (None = all numeric columns)
        
    Returns:
        DataFrame: Cleaned DataFrame
    """
    logger.info(f"Handling missing values using strategy: {strategy}")
    
    if columns is None:
        # Get all numeric columns
        columns = [field.name for field in df.schema.fields 
                  if str(field.dataType) in ['FloatType', 'DoubleType', 'IntegerType']]
    
    df_cleaned = df
    
    if strategy == "mean":
        # Fill with mean for each numeric column
        for column in columns:
            mean_value = df.select(avg(col(column))).first()[0]
            if mean_value is not None:
                df_cleaned = df_cleaned.fillna({column: float(mean_value)})
                
    elif strategy == "median":
        # Fill with median for each numeric column
        for column in columns:
            median_value = df.approxQuantile(column, [0.5], 0.01)[0]
            df_cleaned = df_cleaned.fillna({column: float(median_value)})
            
    elif strategy == "drop":
        # Drop rows with any null values in specified columns
        df_cleaned = df.dropna(subset=columns)
        
    elif strategy == "fill":
        # Fill with 0
        df_cleaned = df.fillna(0, subset=columns)
    
    records_before = df.count()
    records_after = df_cleaned.count()
    logger.info(f"Records before: {records_before}, after: {records_after}")
    
    return df_cleaned


def convert_date_column(df, date_column="Date", date_format=None):
    """
    Convert string date column to DateType with automatic format detection
    
    Args:
        df (DataFrame): Input DataFrame
        date_column (str): Name of the date column
        date_format (str): Format of the date string (None for auto-detect)
        
    Returns:
        DataFrame: DataFrame with converted date column
    """
    from pyspark.sql.functions import to_date, regexp_replace
    
    logger.info(f"Converting {date_column} to DateType")
    
    # If no format specified, try common formats
    if date_format is None:
        # Sample a few dates to detect format
        sample_dates = df.select(date_column).limit(5).collect()
        sample_date = str(sample_dates[0][0]) if sample_dates else ""
        
        # Detect format based on sample
        if '-' in sample_date:
            # Try dd-MM-yyyy format
            date_format = "dd-MM-yyyy"
        elif '/' in sample_date:
            parts = sample_date.split('/')
            if len(parts[0]) == 2:  # dd/MM/yyyy
                date_format = "dd/MM/yyyy"
            else:  # MM/dd/yyyy
                date_format = "MM/dd/yyyy"
        else:
            date_format = "yyyy-MM-dd"
        
        logger.info(f"Auto-detected date format: {date_format}")
    
    # Try to convert with specified format
    try:
        df_with_date = df.withColumn(
            date_column,
            to_date(col(date_column), date_format)
        )
        
        # Check if conversion worked
        null_count = df_with_date.filter(col(date_column).isNull()).count()
        if null_count > 0:
            logger.warning(f"Date conversion resulted in {null_count} null values")
    
    except Exception as e:
        logger.error(f"Error converting dates with format {date_format}: {e}")
        # Try alternative formats
        for alt_format in ["dd-MM-yyyy", "MM-dd-yyyy", "dd/MM/yyyy", "MM/dd/yyyy", "yyyy-MM-dd"]:
            try:
                logger.info(f"Trying alternative format: {alt_format}")
                df_with_date = df.withColumn(
                    date_column,
                    to_date(col(date_column), alt_format)
                )
                # Check if this worked
                null_count = df_with_date.filter(col(date_column).isNull()).count()
                if null_count == 0:
                    logger.info(f"Successfully converted dates with format: {alt_format}")
                    break
            except:
                continue
    
    # Add additional date features
    df_with_date = df_with_date \
        .withColumn("Year", year(col(date_column))) \
        .withColumn("Month", month(col(date_column))) \
        .withColumn("Day", dayofmonth(col(date_column))) \
        .withColumn("DayOfWeek", dayofweek(col(date_column)))
    
    return df_with_date


def detect_outliers(df, column, method="iqr", threshold=1.5):
    """
    Detect outliers in a numeric column
    
    Args:
        df (DataFrame): Input DataFrame
        column (str): Column name to check for outliers
        method (str): Method to detect outliers (iqr, zscore)
        threshold (float): Threshold for outlier detection
        
    Returns:
        DataFrame: DataFrame with outlier flag
    """
    logger.info(f"Detecting outliers in {column} using {method} method")
    
    if method == "iqr":
        # Calculate quartiles
        quantiles = df.approxQuantile(column, [0.25, 0.75], 0.01)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        
        lower_bound = q1 - (threshold * iqr)
        upper_bound = q3 + (threshold * iqr)
        
        # Flag outliers
        df_with_outliers = df.withColumn(
            f"{column}_outlier",
            when((col(column) < lower_bound) | (col(column) > upper_bound), 1).otherwise(0)
        )
        
        outlier_count = df_with_outliers.filter(col(f"{column}_outlier") == 1).count()
        logger.info(f"Found {outlier_count} outliers in {column}")
        logger.info(f"Bounds: [{lower_bound:.2f}, {upper_bound:.2f}]")
        
        return df_with_outliers
    
    elif method == "zscore":
        # Calculate mean and standard deviation
        stats = df.select(avg(col(column)).alias("mean"), 
                         stddev(col(column)).alias("stddev")).first()
        mean_val, stddev_val = stats["mean"], stats["stddev"]
        
        # Flag outliers (z-score > threshold)
        df_with_outliers = df.withColumn(
            f"{column}_outlier",
            when(
                ((col(column) - mean_val) / stddev_val).cast("float").abs() > threshold,
                1
            ).otherwise(0)
        )
        
        outlier_count = df_with_outliers.filter(col(f"{column}_outlier") == 1).count()
        logger.info(f"Found {outlier_count} outliers in {column}")
        
        return df_with_outliers


def handle_outliers(df, column, method="cap", lower_percentile=0.01, upper_percentile=0.99):
    """
    Handle outliers using specified method
    
    Args:
        df (DataFrame): Input DataFrame
        column (str): Column name
        method (str): Method to handle outliers (cap, remove)
        lower_percentile (float): Lower percentile for capping
        upper_percentile (float): Upper percentile for capping
        
    Returns:
        DataFrame: DataFrame with handled outliers
    """
    if method == "cap":
        # Cap at percentiles
        bounds = df.approxQuantile(column, [lower_percentile, upper_percentile], 0.01)
        lower_bound, upper_bound = bounds[0], bounds[1]
        
        df_handled = df.withColumn(
            column,
            when(col(column) < lower_bound, lower_bound)
            .when(col(column) > upper_bound, upper_bound)
            .otherwise(col(column))
        )
        
        logger.info(f"Capped {column} at [{lower_bound:.2f}, {upper_bound:.2f}]")
        
    elif method == "remove":
        # Remove outliers
        bounds = df.approxQuantile(column, [lower_percentile, upper_percentile], 0.01)
        lower_bound, upper_bound = bounds[0], bounds[1]
        
        records_before = df.count()
        df_handled = df.filter(
            (col(column) >= lower_bound) & (col(column) <= upper_bound)
        )
        records_after = df_handled.count()
        
        logger.info(f"Removed {records_before - records_after} outlier records")
    
    else:
        df_handled = df
    
    return df_handled


def validate_data(df):
    """
    Perform data validation checks
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        dict: Validation results
    """
    validation_results = {
        "total_records": df.count(),
        "duplicate_records": df.count() - df.dropDuplicates().count(),
        "null_percentage": {}
    }
    
    # Check null percentage for each column
    total_records = df.count()
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_pct = (null_count / total_records) * 100
        validation_results["null_percentage"][column] = round(null_pct, 2)
    
    print("\n=== Data Validation Results ===")
    print(f"Total Records: {validation_results['total_records']}")
    print(f"Duplicate Records: {validation_results['duplicate_records']}")
    print("\nNull Percentage by Column:")
    for col_name, pct in validation_results["null_percentage"].items():
        print(f"  {col_name}: {pct}%")
    
    return validation_results


def clean_walmart_data(df):
    """
    Complete data cleaning pipeline for Walmart sales data
    
    Args:
        df (DataFrame): Raw Walmart sales DataFrame
        
    Returns:
        DataFrame: Cleaned DataFrame
    """
    logger.info("Starting data cleaning pipeline...")
    
    # 1. Check initial data quality
    check_null_values(df)
    
    # 2. Convert date column (auto-detect format)
    df_cleaned = convert_date_column(df, date_column="Date", date_format=None)
    
    # 3. Handle missing values
    numeric_columns = ["Weekly_Sales", "Temperature", "Fuel_Price", "CPI", "Unemployment"]
    df_cleaned = handle_missing_values(df_cleaned, strategy="mean", columns=numeric_columns)
    
    # 4. Handle outliers in Weekly_Sales
    df_cleaned = handle_outliers(df_cleaned, column="Weekly_Sales", method="cap")
    
    # 5. Remove duplicates
    records_before = df_cleaned.count()
    df_cleaned = df_cleaned.dropDuplicates()
    records_after = df_cleaned.count()
    logger.info(f"Removed {records_before - records_after} duplicate records")
    
    # 6. Validate cleaned data
    validate_data(df_cleaned)
    
    logger.info("Data cleaning pipeline completed")
    
    return df_cleaned
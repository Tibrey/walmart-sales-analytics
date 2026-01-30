"""
This module performs the feature engineering and data transformations for sales analytics

"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, lag, lead, sum as _sum, avg, max as _max, min as _min,
    count, round as _round, when, datediff, lit, concat, 
    year, month, quarter, weekofyear, dayofweek, date_format
)
import logging

logger = logging.getLogger(__name__)


def add_temporal_features(df, date_column="Date"):
    """
    Add comprehensive temporal features for time series analysis

    Args:
        df (DataFrame): Input DataFrame with date column
        date_column (str): Name of date column
        
    Returns:
        DataFrame: DataFrame with additional temporal features
    """
    logger.info("Adding temporal features...")

    df_temporal = df \
        .withColumn("Year", year(col(date_column))) \
        .withColumn("Quarter", quarter(col(date_column))) \
        .withColumn("Month", month(col(date_column))) \
        .withColumn("WeekOfYear", weekofyear(col(date_column))) \
        .withColumn("DayOfWeek", dayofweek(col(date_column))) \
        .withColumn("MonthName", date_format(col(date_column), "MMMM")) \
        .withColumn("DayName", date_format(col(date_column), "EEEE"))
    
    # Add weekend flag
    df_temporal = df_temporal.withColumn(
        "IsWeekend",
        when(col("DayOfWeek").isin([1,7]),1).otherwise(0)
    )

    # Add season
    df_temporal = df_temporal.withColumn(
        "Season",
        when(col("Month").isin([12, 1, 2]), "Winter")
        .when(col("Month").isin([3, 4, 5]), "Spring")
        .when(col("Month").isin([6, 7, 8]), "Summer")
        .otherwise("Fall")
    )

    logger.info("Temporal features added successfully")
    return df_temporal

def calculate_rolling_metrics(df,partition_cols=["Store"], order_col="Date",
                              value_col="Weekly_Sales", windows=[4,12,26]):
    """
    Calculate rolling window metrices (moving averages, sums)

    Args:
        df (DataFrame): Input DataFrame
        partition_cols (list): Columns to partition by
        order_col (str): Column by order by
        value_col (str): Column to calculate metrics on
        windows (list): List of window sizes (in weeks)

    Returns:
        DataFrame: DataFrame with rolling metrics
    """

    logger.info(f"Calculating rolling metrics for windows:{windows}")

    df_rolling = df

    for window_size in windows:
        # Define window specification
        window_spec = Window.partitionBy(*partition_cols) \
                            .orderBy(col(order_col)) \
                            .rowsBetween(-window_size + 1, 0)
        
        # Calculate rolling average
        df_rolling = df_rolling.withColumn(
            f"Rolling_Avg_{window_size}W",
            _round(avg(col(value_col)).over(window_spec),2)
        )

        # Calculate rolling sum
        df_rolling = df_rolling.withColumn(
            f"Rolling_Sum_{window_size}W",
            _round(avg(col(value_col)).over(window_spec),2)
        )

    logger.info("Rounding metrics calculated successfully")
    return df_rolling


def calculate_lag_features(df, partition_cols=["Store"], order_col="Date",
                           value_col="Weekly_Sales", lags=[1,4,52]):
    """
    Calculate lag features for time series
    
    Args:
        df (DataFrame): Input DataFrame
        partition_cols (list): Columns to partition by
        order_col (str): Column to order by
        value_col (str): Column to calculate lags on
        lags (list): List of lag periods
        
    Returns:
    """
    logger.info(f"Calculating lag features for periods: {lags}")

    df_lagged =df

    window_spec = Window.partitionBy(*partition_cols).orderBy(col(order_col))

    for lag_period in lags:
        df_lagged = df_lagged.withColumn(
            f"Sales_Lag_{lag_period}W",
            lag(col(value_col), lag_period).over(window_spec)
        )
    
    logger.info("Lag features calculated successfully")
    return df_lagged

def calculate_growth_rates(df, partition_cols=["Store"], order_col="Date",
                           value_col="Weekly_Sales"):
    """
    Calculate period-over-period growth rates
    
    Args:
        df (DataFrame): Input DataFrame
        partition_cols (list): Columns to partition by
        order_col (str): Column to order by
        value_col (str): Column to calculate growth on
        
    Returns:
        DataFrame: DataFrame with growth rate metrics
    """
    logger.info("Calculating growth rates...")

    window_spec = Window.partitionBy(*partition_cols).orderBy(col(order_col))

    # Week-over-week growth
    df_growth = df.withColumn(
        "Sales_Prev_Week",
        lag(col(value_col),1).over(window_spec)
    )
    df_growth = df_growth.withColumn(
        "WoW_Growth_Pct",
        _round(
            ((col(value_col) - col("Sales_Prev_Week")) / col("Sales_Prev_Week")) * 100,
            2
        )
    )
    
    # Year-over-year growth (52 weeks)
    df_growth = df_growth.withColumn(
        "Sales_Prev_Year",
        lag(col(value_col), 52).over(window_spec)
    )
    
    df_growth = df_growth.withColumn(
        "YoY_Growth_Pct",
        _round(
            ((col(value_col) - col("Sales_Prev_Year")) / col("Sales_Prev_Year")) * 100,
            2
        )
    )
    
    logger.info("Growth rates calculated successfully")
    return df_growth

def calculate_cumulative_metrics(df, partition_cols=["Store", "Year"], 
                                 order_col="Date", value_col="Weekly_Sales"):
    """
    Calculate cumulative metrics
    
    Args:
        df (DataFrame): Input DataFrame
        partition_cols (list): Columns to partition by
        order_col (str): Column to order by
        value_col (str): Column to calculate cumulative metrics on
        
    Returns:
        DataFrame: DataFrame with cumulative metrics
    """
    logger.info("Calculating cumulative metrics...")
    
    window_spec = Window.partitionBy(*partition_cols) \
                        .orderBy(col(order_col)) \
                        .rowsBetween(Window.unboundedPreceding, 0)
    
    df_cumulative = df.withColumn(
        "Cumulative_Sales",
        _round(_sum(col(value_col)).over(window_spec), 2)
    )
    
    df_cumulative = df_cumulative.withColumn(
        "Cumulative_Avg_Sales",
        _round(avg(col(value_col)).over(window_spec), 2)
    )
    
    logger.info("Cumulative metrics calculated successfully")
    return df_cumulative

def add_sales_categories(df, sales_col="Weekly_Sales"):
    """
    Categorize sales into performance buckets
    
    Args:
        df (DataFrame): Input DataFrame
        sales_col (str): Sales column name
        
    Returns:
        DataFrame: DataFrame with sales categories
    """
    logger.info("Adding sales performance categories...")
    
    # Calculate quartiles
    quantiles = df.approxQuantile(sales_col, [0.25, 0.5, 0.75], 0.01)
    q1, q2, q3 = quantiles[0], quantiles[1], quantiles[2]
    
    df_categorized = df.withColumn(
        "Sales_Category",
        when(col(sales_col) <= q1, "Low")
        .when((col(sales_col) > q1) & (col(sales_col) <= q2), "Medium-Low")
        .when((col(sales_col) > q2) & (col(sales_col) <= q3), "Medium-High")
        .otherwise("High")
    )
    
    df_categorized = df_categorized.withColumn(
        "Sales_Quartile",
        when(col(sales_col) <= q1, 1)
        .when((col(sales_col) > q1) & (col(sales_col) <= q2), 2)
        .when((col(sales_col) > q2) & (col(sales_col) <= q3), 3)
        .otherwise(4)
    )
    
    logger.info(f"Sales categories added (Quartiles: {q1:.2f}, {q2:.2f}, {q3:.2f})")
    return df_categorized


def create_feature_set(df):
    """
    Create complete feature set with all transformations
    
    Args:
        df (DataFrame): Cleaned input DataFrame
        
    Returns:
        DataFrame: DataFrame with complete feature set
    """
    logger.info("Creating complete feature set...")
    
    # Add temporal features
    df_features = add_temporal_features(df)
    
    # Add rolling metrics
    df_features = calculate_rolling_metrics(df_features, windows=[4, 12, 26])
    
    # Add lag features
    df_features = calculate_lag_features(df_features, lags=[1, 4, 52])
    
    # Add growth rates
    df_features = calculate_growth_rates(df_features)
    
    # Add cumulative metrics
    df_features = calculate_cumulative_metrics(df_features)
    
    # Add sales categories
    df_features = add_sales_categories(df_features)
    
    logger.info("Feature set creation completed")
    
    return df_features


def aggregate_by_store(df):
    """
    Aggregate sales data by store
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        DataFrame: Aggregated DataFrame by store
    """
    df_store_agg = df.groupBy("Store").agg(
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        _round(avg("Weekly_Sales"), 2).alias("Avg_Weekly_Sales"),
        _round(_max("Weekly_Sales"), 2).alias("Max_Weekly_Sales"),
        _round(_min("Weekly_Sales"), 2).alias("Min_Weekly_Sales"),
        count("*").alias("Total_Weeks")
    )
    
    return df_store_agg


def aggregate_by_time(df, time_period="Month"):
    """
    Aggregate sales data by time period
    
    Args:
        df (DataFrame): Input DataFrame
        time_period (str): Time period to aggregate by (Month, Quarter, Year)
        
    Returns:
        DataFrame: Aggregated DataFrame by time period
    """
    if time_period == "Month":
        group_cols = ["Year", "Month", "MonthName"]
    elif time_period == "Quarter":
        group_cols = ["Year", "Quarter"]
    else:
        group_cols = ["Year"]
    
    df_time_agg = df.groupBy(group_cols).agg(
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        _round(avg("Weekly_Sales"), 2).alias("Avg_Weekly_Sales"),
        count("*").alias("Total_Records")
    ).orderBy("Year", *group_cols[1:] if len(group_cols) > 1 else [])
    
    return df_time_agg


# Example usage
# if __name__ == "__main__":
#     from spark_session import create_spark_session, stop_spark_session
#     from data_ingestion import load_walmart_sales_data
#     from data_cleaning import clean_walmart_data
    
#     spark = create_spark_session()
    
#     try:
#         # Load and clean data
#         df = load_walmart_sales_data(spark, "../data/raw/Walmart_Sales.csv")
#         df_cleaned = clean_walmart_data(df)
        
#         # Create feature set
#         df_features = create_feature_set(df_cleaned)
        
#         # Show sample with features
#         print("\n=== Data with Features ===")
#         df_features.select(
#             "Store", "Date", "Weekly_Sales", "Year", "Month", "Season",
#             "Rolling_Avg_4W", "WoW_Growth_Pct", "Sales_Category"
#         ).show(10)
        
#     finally:
#         stop_spark_session(spark)

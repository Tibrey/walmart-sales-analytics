"""
This module performs advanced time series analytics for sales forecasting and trend analysis
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, lag, avg, sum as _sum, round as _round, count,
    when, datediff, months_between, year, month, row_number
)
import logging

logger = logging.getLogger(__name__)


def calculate_moving_averages(df, windows=[4, 8, 12, 26, 52]):
    """
    Calculate multiple moving averages for trend analysis
    
    Args:
        df (DataFrame): Input DataFrame with time series data
        windows (list): List of window sizes (in weeks)
        
    Returns:
        DataFrame: DataFrame with moving averages
    """
    logger.info(f"Calculating moving averages for windows: {windows}")
    
    df_ma = df
    window_spec = Window.partitionBy("Store").orderBy("Date")
    
    for window_size in windows:
        window_spec_sized = window_spec.rowsBetween(-window_size + 1, 0)
        
        df_ma = df_ma.withColumn(
            f"MA_{window_size}W",
            _round(avg("Weekly_Sales").over(window_spec_sized), 2)
        )
    
    logger.info("Moving averages calculated successfully")
    return df_ma


def detect_trends(df, short_window=4, long_window=12):
    """
    Detect trends using moving average crossovers
    
    Args:
        df (DataFrame): Input DataFrame with moving averages
        short_window (int): Short-term moving average window
        long_window (int): Long-term moving average window
        
    Returns:
        DataFrame: DataFrame with trend indicators
    """
    logger.info("Detecting sales trends...")
    
    # Ensure moving averages exist
    if f"MA_{short_window}W" not in df.columns:
        df = calculate_moving_averages(df, windows=[short_window, long_window])
    
    # Detect trend (uptrend when short MA > long MA)
    df_trends = df.withColumn(
        "Trend_Signal",
        when(col(f"MA_{short_window}W") > col(f"MA_{long_window}W"), "Uptrend")
        .when(col(f"MA_{short_window}W") < col(f"MA_{long_window}W"), "Downtrend")
        .otherwise("Neutral")
    )
    
    logger.info("Trend detection completed")
    return df_trends


def calculate_momentum(df, period=4):
    """
    Calculate momentum indicator
    
    Args:
        df (DataFrame): Input DataFrame
        period (int): Period for momentum calculation
        
    Returns:
        DataFrame: DataFrame with momentum indicator
    """
    logger.info(f"Calculating {period}-week momentum...")
    
    window_spec = Window.partitionBy("Store").orderBy("Date")
    
    df_momentum = df.withColumn(
        f"Sales_{period}W_Ago",
        lag("Weekly_Sales", period).over(window_spec)
    )
    
    df_momentum = df_momentum.withColumn(
        f"Momentum_{period}W",
        _round(col("Weekly_Sales") - col(f"Sales_{period}W_Ago"), 2)
    )
    
    df_momentum = df_momentum.withColumn(
        f"Momentum_Pct_{period}W",
        _round(
            (col(f"Momentum_{period}W") / col(f"Sales_{period}W_Ago")) * 100,
            2
        )
    )
    
    logger.info("Momentum calculation completed")
    return df_momentum


def identify_seasonality(df):
    """
    Identify seasonal patterns in sales data
    
    Args:
        df (DataFrame): Input DataFrame with temporal features
        
    Returns:
        DataFrame: Seasonality analysis by month and quarter
    """
    logger.info("Identifying seasonal patterns...")
    
    # Monthly seasonality
    monthly_seasonality = df.groupBy("Month", "MonthName").agg(
        _round(avg("Weekly_Sales"), 2).alias("Avg_Sales"),
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        count("*").alias("Data_Points")
    ).orderBy("Month")
    
    print("\n=== Monthly Seasonality Pattern ===")
    monthly_seasonality.show(12)
    
    # Quarterly seasonality
    quarterly_seasonality = df.groupBy("Quarter").agg(
        _round(avg("Weekly_Sales"), 2).alias("Avg_Sales"),
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        count("*").alias("Data_Points")
    ).orderBy("Quarter")
    
    print("\n=== Quarterly Seasonality Pattern ===")
    quarterly_seasonality.show()
    
    # Day of week seasonality
    dow_seasonality = df.groupBy("DayOfWeek", "DayName").agg(
        _round(avg("Weekly_Sales"), 2).alias("Avg_Sales"),
        count("*").alias("Data_Points")
    ).orderBy("DayOfWeek")
    
    print("\n=== Day of Week Pattern ===")
    dow_seasonality.show()
    
    return {
        "monthly": monthly_seasonality,
        "quarterly": quarterly_seasonality,
        "day_of_week": dow_seasonality
    }


def calculate_yoy_metrics(df):
    """
    Calculate year-over-year comparison metrics
    
    Args:
        df (DataFrame): Input DataFrame with temporal features
        
    Returns:
        DataFrame: YoY comparison by month
    """
    logger.info("Calculating year-over-year metrics...")
    
    # Group by year and month
    yoy_data = df.groupBy("Year", "Month", "MonthName").agg(
        _round(_sum("Weekly_Sales"), 2).alias("Monthly_Sales"),
        _round(avg("Weekly_Sales"), 2).alias("Avg_Weekly_Sales"),
        count("*").alias("Week_Count")
    ).orderBy("Year", "Month")
    
    # Calculate previous year values
    window_spec = Window.partitionBy("Month").orderBy("Year")
    
    yoy_data = yoy_data.withColumn(
        "Prev_Year_Sales",
        lag("Monthly_Sales", 1).over(window_spec)
    )
    
    yoy_data = yoy_data.withColumn(
        "YoY_Change",
        _round(col("Monthly_Sales") - col("Prev_Year_Sales"), 2)
    )
    
    yoy_data = yoy_data.withColumn(
        "YoY_Change_Pct",
        _round(
            (col("YoY_Change") / col("Prev_Year_Sales")) * 100,
            2
        )
    )
    
    print("\n=== Year-over-Year Monthly Comparison ===")
    yoy_data.select("Year", "MonthName", "Monthly_Sales", 
                    "Prev_Year_Sales", "YoY_Change_Pct").show(24)
    
    return yoy_data


def analyze_peak_periods(df, top_n=10):
    """
    Identify peak sales periods
    
    Args:
        df (DataFrame): Input DataFrame
        top_n (int): Number of top periods to identify
        
    Returns:
        DataFrame: Top performing time periods
    """
    logger.info(f"Analyzing top {top_n} peak sales periods...")
    
    # Overall peaks
    peak_weeks = df.select(
        "Store", "Date", "Weekly_Sales", "Holiday_Flag", 
        "Year", "Month", "MonthName", "Season"
    ).orderBy(col("Weekly_Sales").desc()).limit(top_n)
    
    print(f"\n=== Top {top_n} Peak Sales Weeks ===")
    peak_weeks.show(top_n, truncate=False)
    
    # Peak months
    peak_months = df.groupBy("Year", "Month", "MonthName").agg(
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales")
    ).orderBy(col("Total_Sales").desc()).limit(top_n)
    
    print(f"\n=== Top {top_n} Peak Sales Months ===")
    peak_months.show(top_n)
    
    return {
        "peak_weeks": peak_weeks,
        "peak_months": peak_months
    }


def calculate_sales_velocity(df):
    """
    Calculate rate of change in sales (velocity)
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        DataFrame: DataFrame with sales velocity metrics
    """
    logger.info("Calculating sales velocity...")
    
    window_spec = Window.partitionBy("Store").orderBy("Date")
    
    # Calculate week-over-week change
    df_velocity = df.withColumn(
        "Prev_Week_Sales",
        lag("Weekly_Sales", 1).over(window_spec)
    )
    
    df_velocity = df_velocity.withColumn(
        "Sales_Velocity",
        _round(col("Weekly_Sales") - col("Prev_Week_Sales"), 2)
    )
    
    df_velocity = df_velocity.withColumn(
        "Sales_Acceleration",
        _round(
            col("Sales_Velocity") - lag("Sales_Velocity", 1).over(window_spec),
            2
        )
    )
    
    # Categorize velocity
    df_velocity = df_velocity.withColumn(
        "Velocity_Status",
        when(col("Sales_Velocity") > 0, "Accelerating")
        .when(col("Sales_Velocity") < 0, "Decelerating")
        .otherwise("Stable")
    )
    
    logger.info("Sales velocity calculation completed")
    return df_velocity


def forecast_baseline(df, forecast_periods=4):
    """
    Create simple baseline forecast using moving average
    
    Args:
        df (DataFrame): Input DataFrame
        forecast_periods (int): Number of periods to forecast
        
    Returns:
        DataFrame: Forecast summary by store
    """
    logger.info(f"Creating baseline forecast for {forecast_periods} periods...")
    
    # Calculate recent average for each store (last 12 weeks)
    window_spec = Window.partitionBy("Store").orderBy(col("Date").desc())
    
    recent_data = df.withColumn(
        "row_num",
        row_number().over(window_spec)
    ).filter(col("row_num") <= 12)
    
    forecast_baseline = recent_data.groupBy("Store").agg(
        _round(avg("Weekly_Sales"), 2).alias("Recent_Avg_Sales"),
        _round(avg("MA_4W"), 2).alias("Recent_MA_4W"),
        _round(avg("MA_12W"), 2).alias("Recent_MA_12W")
    )
    
    # Simple forecast = recent average
    forecast_baseline = forecast_baseline.withColumn(
        "Forecast_Next_Week",
        _round((col("Recent_MA_4W") + col("Recent_MA_12W")) / 2, 2)
    )
    
    print("\n=== Baseline Forecast by Store ===")
    forecast_baseline.orderBy(col("Forecast_Next_Week").desc()).show(10)
    
    return forecast_baseline


def analyze_trend_strength(df):
    """
    Analyze strength of trends across stores
    
    Args:
        df (DataFrame): Input DataFrame with trend indicators
        
    Returns:
        DataFrame: Trend strength analysis
    """
    logger.info("Analyzing trend strength...")
    
    trend_analysis = df.groupBy("Store", "Trend_Signal").agg(
        count("*").alias("Period_Count")
    ).orderBy("Store", "Trend_Signal")
    
    # Calculate trend consistency
    store_trends = df.groupBy("Store").agg(
        count(when(col("Trend_Signal") == "Uptrend", 1)).alias("Uptrend_Periods"),
        count(when(col("Trend_Signal") == "Downtrend", 1)).alias("Downtrend_Periods"),
        count(when(col("Trend_Signal") == "Neutral", 1)).alias("Neutral_Periods"),
        count("*").alias("Total_Periods")
    )
    
    store_trends = store_trends.withColumn(
        "Uptrend_Pct",
        _round((col("Uptrend_Periods") / col("Total_Periods")) * 100, 2)
    )
    
    print("\n=== Trend Strength by Store ===")
    store_trends.orderBy(col("Uptrend_Pct").desc()).show(10)
    
    return store_trends
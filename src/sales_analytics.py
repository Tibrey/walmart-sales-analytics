"""
This module performs comprehensive sales analysis and generates business insights
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as _sum, avg, max as _max, min as _min, count,
    round as _round, desc, asc, when, rank, dense_rank, lag
)
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)

def analyze_store_performance(df):
    """
    Comprehensive store performance analysis
    
    Args:
        df (DataFrame): Input DataFrame with sales data
        
    Returns:
        DataFrame: Store performance metrics
    """
    logger.info("Analyzing store performance...")

    store_performance =df.groupBy("Store").agg(
        _round(_sum("Weekly_Sales"),2).alias("Total_Sales"),
        _round(avg("Weekly_Sales"), 2).alias("Avg_Weekly_Sales"),
        _round(_max("Weekly_Sales"), 2).alias("Peak_Sales"),
        _round(_min("Weekly_Sales"), 2).alias("Lowest_Sales"),
        count("*").alias("Total_Weeks"),
        _round(avg("Temperature"), 2).alias("Avg_Temperature"),
        _round(avg("Fuel_Price"), 2).alias("Avg_Fuel_Price"),
        _round(avg("CPI"), 2).alias("Avg_CPI"),
        _round(avg("Unemployment"), 2).alias("Avg_Unemployment")
    )

    # Add rank based on total sales
    window_spec = Window.orderBy(desc("Total_Sales"))
    store_performance = store_performance.withColumn(
        "Sales_Rank",
        rank().over(window_spec)
    )

    # Calculate sales variance
    store_performance = store_performance.withColumn(
        "Sales_Range",
        _round(col("Peak_Sales") - col("Lowest_Sales"), 2)
    )
    
    logger.info("Store performance analysis completed")
    return store_performance.orderBy("Sales_Rank")

def identify_top_bottom_stores(df, top_n=10):
    """
    Identify top and bottom performing stores
    
    Args:
        df (DataFrame): Input DataFrame
        top_n (int): Number of top/bottom stores to return
        
    Returns:
        tuple: (top_stores DataFrame, bottom_stores DataFrame)
    """
    store_totals = df.groupBy("Store").agg(
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales")
    )
    
    top_stores = store_totals.orderBy(desc("Total_Sales")).limit(top_n)
    bottom_stores = store_totals.orderBy(asc("Total_Sales")).limit(top_n)
    
    print(f"\n=== Top {top_n} Performing Stores ===")
    top_stores.show()
    
    print(f"\n=== Bottom {top_n} Performing Stores ===")
    bottom_stores.show()
    
    return top_stores, bottom_stores

def analyze_holiday_impact(df):
    """
    Analyze impact of holidays on sales
    
    Args:
        df (DataFrame): Input DataFrame with Holiday_Flag
        
    Returns:
        DataFrame: Holiday vs non-holiday sales comparison
    """
    logger.info("Analyzing holiday impact on sales...")
    
    holiday_analysis = df.groupBy("Holiday_Flag").agg(
        count("*").alias("Total_Records"),
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        _round(avg("Weekly_Sales"), 2).alias("Avg_Sales"),
        _round(_max("Weekly_Sales"), 2).alias("Max_Sales"),
        _round(_min("Weekly_Sales"), 2).alias("Min_Sales")
    )
    
    holiday_analysis = holiday_analysis.withColumn(
        "Period_Type",
        when(col("Holiday_Flag") == 1, "Holiday").otherwise("Non-Holiday")
    )
    
    # Calculate holiday uplift percentage
    non_holiday_avg = holiday_analysis.filter(col("Holiday_Flag") == 0) \
                                      .select("Avg_Sales").first()[0]
    holiday_avg = holiday_analysis.filter(col("Holiday_Flag") == 1) \
                                  .select("Avg_Sales").first()[0]
    
    if non_holiday_avg and holiday_avg:
        uplift_pct = ((holiday_avg - non_holiday_avg) / non_holiday_avg) * 100
        logger.info(f"Holiday sales uplift: {uplift_pct:.2f}%")
    
    print("\n=== Holiday Impact Analysis ===")
    holiday_analysis.select("Period_Type", "Total_Records", "Total_Sales", 
                           "Avg_Sales", "Max_Sales").show()
    
    return holiday_analysis

def analyze_seasonal_trends(df):
    """
    Analyze sales by season
    
    Args:
        df (DataFrame): Input DataFrame with Season column
        
    Returns:
        DataFrame: Seasonal sales analysis
    """
    logger.info("Analyzing seasonal trends...")
    
    seasonal_analysis = df.groupBy("Season").agg(
        count("*").alias("Total_Records"),
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        _round(avg("Weekly_Sales"), 2).alias("Avg_Sales"),
        _round(_max("Weekly_Sales"), 2).alias("Max_Sales")
    ).orderBy(desc("Total_Sales"))
    
    print("\n=== Seasonal Sales Analysis ===")
    seasonal_analysis.show()
    
    return seasonal_analysis

def analyze_monthly_trends(df):
    """
    Analyze sales trends by month
    
    Args:
        df (DataFrame): Input DataFrame with temporal features
        
    Returns:
        DataFrame: Monthly sales analysis
    """
    logger.info("Analyzing monthly trends...")
    
    monthly_analysis = df.groupBy("Year", "Month", "MonthName").agg(
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        _round(avg("Weekly_Sales"), 2).alias("Avg_Sales"),
        count("*").alias("Total_Records")
    ).orderBy("Year", "Month")
    
    print("\n=== Monthly Sales Trends (Last 12 Months) ===")
    monthly_analysis.orderBy(desc("Year"), desc("Month")).limit(12).show()
    
    return monthly_analysis

def analyze_yearly_trends(df):
    """
    Analyze year-over-year sales trends
    
    Args:
        df (DataFrame): Input DataFrame with Year column
        
    Returns:
        DataFrame: Yearly sales analysis with YoY growth
    """
    logger.info("Analyzing yearly trends...")
    
    yearly_analysis = df.groupBy("Year").agg(
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        _round(avg("Weekly_Sales"), 2).alias("Avg_Weekly_Sales"),
        count("*").alias("Total_Weeks")
    ).orderBy("Year")
    
    # Calculate YoY growth
    window_spec = Window.orderBy("Year")
    yearly_analysis = yearly_analysis.withColumn(
        "Prev_Year_Sales",
        lag(col("Total_Sales"), 1).over(window_spec)
    )
    
    yearly_analysis = yearly_analysis.withColumn(
        "YoY_Growth_Pct",
        _round(
            ((col("Total_Sales") - col("Prev_Year_Sales")) / col("Prev_Year_Sales")) * 100,
            2
        )
    )
    
    print("\n=== Year-over-Year Sales Analysis ===")
    yearly_analysis.select("Year", "Total_Sales", "Avg_Weekly_Sales", 
                          "YoY_Growth_Pct").show()
    
    return yearly_analysis

def analyze_external_factors(df):
    """
    Analyze correlation between sales and external factors
    
    Args:
        df (DataFrame): Input DataFrame with external factors
        
    Returns:
        dict: Statistical analysis of external factors
    """
    logger.info("Analyzing external factors impact...")
    
    
    # Temperature impact
    temp_analysis = df.groupBy("Store").agg(
        avg("Temperature").alias("Avg_Temp"),
        avg("Weekly_Sales").alias("Avg_Sales")
    )
    print("\n=== Temperature vs Sales (by Store) ===")
    temp_analysis.orderBy(desc("Avg_Sales")).limit(10).show()
    
    # Fuel Price impact
    fuel_analysis = df.groupBy("Store").agg(
        avg("Fuel_Price").alias("Avg_Fuel_Price"),
        avg("Weekly_Sales").alias("Avg_Sales")
    )
    print("\n=== Fuel Price vs Sales (by Store) ===")
    fuel_analysis.orderBy(desc("Avg_Sales")).limit(10).show()
    
    # Unemployment impact
    unemployment_analysis = df.groupBy("Store").agg(
        avg("Unemployment").alias("Avg_Unemployment"),
        avg("Weekly_Sales").alias("Avg_Sales")
    )
    print("\n=== Unemployment vs Sales (by Store) ===")
    unemployment_analysis.orderBy(desc("Avg_Sales")).limit(10).show()
    
    return {
        "temperature": temp_analysis,
        "fuel_price": fuel_analysis,
        "unemployment": unemployment_analysis
    }

def generate_sales_summary(df):
    """
    Generate comprehensive sales summary report
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        dict: Summary statistics
    """
    logger.info("Generating sales summary report...")
    
    total_sales = df.agg(_sum("Weekly_Sales")).first()[0]
    avg_sales = df.agg(avg("Weekly_Sales")).first()[0]
    max_sales = df.agg(_max("Weekly_Sales")).first()[0]
    min_sales = df.agg(_min("Weekly_Sales")).first()[0]
    total_records = df.count()
    unique_stores = df.select("Store").distinct().count()
    
    summary = {
        "total_sales": round(total_sales, 2),
        "average_weekly_sales": round(avg_sales, 2),
        "maximum_weekly_sales": round(max_sales, 2),
        "minimum_weekly_sales": round(min_sales, 2),
        "total_records": total_records,
        "unique_stores": unique_stores
    }
    
    print("\n" + "="*50)
    print("WALMART SALES SUMMARY REPORT")
    print("="*50)
    print(f"Total Sales Revenue: ${summary['total_sales']:,.2f}")
    print(f"Average Weekly Sales: ${summary['average_weekly_sales']:,.2f}")
    print(f"Maximum Weekly Sales: ${summary['maximum_weekly_sales']:,.2f}")
    print(f"Minimum Weekly Sales: ${summary['minimum_weekly_sales']:,.2f}")
    print(f"Total Data Points: {summary['total_records']:,}")
    print(f"Number of Stores: {summary['unique_stores']}")
    print("="*50 + "\n")
    
    return summary

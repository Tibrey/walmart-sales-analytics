"""
This module calculate and track key performance indicators for sales analytics
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, sum as _sum, avg, max as _max, min as _min, count, round as _round,
    lag, when, stddev, desc, asc, rank
)
import logging

logger = logging.getLogger(__name__)


def calculate_revenue_metrics(df):
    """
    Calculate comprehensive revenue metrics
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        dict: Revenue KPIs
    """
    logger.info("Calculating revenue metrics...")
    
    metrics = {}
    
    # Total revenue
    metrics['total_revenue'] = df.agg(_sum("Weekly_Sales")).first()[0]
    
    # Average revenue per week
    metrics['avg_weekly_revenue'] = df.agg(avg("Weekly_Sales")).first()[0]
    
    # Average revenue per store
    store_totals = df.groupBy("Store").agg(_sum("Weekly_Sales").alias("Total"))
    metrics['avg_revenue_per_store'] = store_totals.agg(avg("Total")).first()[0]
    
    # Revenue by year
    yearly_revenue = df.groupBy("Year").agg(
        _round(_sum("Weekly_Sales"), 2).alias("Annual_Revenue")
    ).orderBy("Year")
    
    print("\n=== Revenue Metrics ===")
    print(f"Total Revenue: ${metrics['total_revenue']:,.2f}")
    print(f"Average Weekly Revenue: ${metrics['avg_weekly_revenue']:,.2f}")
    print(f"Average Revenue per Store: ${metrics['avg_revenue_per_store']:,.2f}")
    print("\nAnnual Revenue:")
    yearly_revenue.show()
    
    metrics['yearly_revenue'] = yearly_revenue
    
    return metrics


def calculate_growth_metrics(df):
    """
    Calculate growth-related KPIs
    
    Args:
        df (DataFrame): Input DataFrame with temporal features
        
    Returns:
        DataFrame: Growth metrics
    """
    logger.info("Calculating growth metrics...")
    
    # Monthly growth
    monthly_growth = df.groupBy("Year", "Month").agg(
        _round(_sum("Weekly_Sales"), 2).alias("Monthly_Sales")
    ).orderBy("Year", "Month")
    
    window_spec = Window.orderBy("Year", "Month")
    
    monthly_growth = monthly_growth.withColumn(
        "Prev_Month_Sales",
        lag("Monthly_Sales", 1).over(window_spec)
    )
    
    monthly_growth = monthly_growth.withColumn(
        "MoM_Growth_Pct",
        _round(
            ((col("Monthly_Sales") - col("Prev_Month_Sales")) / col("Prev_Month_Sales")) * 100,
            2
        )
    )
    
    # Average growth rate
    avg_growth = monthly_growth.filter(col("MoM_Growth_Pct").isNotNull()) \
                               .agg(avg("MoM_Growth_Pct")).first()[0]
    
    print("\n=== Growth Metrics ===")
    print(f"Average Monthly Growth Rate: {avg_growth:.2f}%")
    print("\nRecent Monthly Growth:")
    monthly_growth.select("Year", "Month", "Monthly_Sales", "MoM_Growth_Pct") \
                  .orderBy(desc("Year"), desc("Month")).limit(12).show()
    
    return monthly_growth


def calculate_store_performance_kpis(df):
    """
    Calculate store-level performance KPIs
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        DataFrame: Store performance KPIs
    """
    logger.info("Calculating store performance KPIs...")
    
    store_kpis = df.groupBy("Store").agg(
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        _round(avg("Weekly_Sales"), 2).alias("Avg_Weekly_Sales"),
        _round(_max("Weekly_Sales"), 2).alias("Peak_Sales"),
        _round(_min("Weekly_Sales"), 2).alias("Lowest_Sales"),
        _round(stddev("Weekly_Sales"), 2).alias("Sales_StdDev"),
        count("*").alias("Data_Points")
    )
    
    # Calculate coefficient of variation (measure of consistency)
    store_kpis = store_kpis.withColumn(
        "Consistency_Score",
        _round(100 - ((col("Sales_StdDev") / col("Avg_Weekly_Sales")) * 100), 2)
    )
    
    # Calculate performance score (normalized)
    max_sales = store_kpis.agg(_max("Total_Sales")).first()[0]
    store_kpis = store_kpis.withColumn(
        "Performance_Score",
        _round((col("Total_Sales") / max_sales) * 100, 2)
    )
    
    # Add performance tier
    store_kpis = store_kpis.withColumn(
        "Performance_Tier",
        when(col("Performance_Score") >= 80, "Excellent")
        .when(col("Performance_Score") >= 60, "Good")
        .when(col("Performance_Score") >= 40, "Average")
        .otherwise("Below Average")
    )
    
    print("\n=== Store Performance KPIs ===")
    store_kpis.select("Store", "Total_Sales", "Avg_Weekly_Sales", 
                     "Consistency_Score", "Performance_Score", "Performance_Tier") \
              .orderBy(desc("Performance_Score")).show(10)
    
    return store_kpis


def calculate_holiday_impact_kpi(df):
    """
    Calculate holiday impact KPIs
    
    Args:
        df (DataFrame): Input DataFrame with Holiday_Flag
        
    Returns:
        dict: Holiday impact metrics
    """
    logger.info("Calculating holiday impact KPIs...")
    
    holiday_kpis = df.groupBy("Holiday_Flag").agg(
        _round(avg("Weekly_Sales"), 2).alias("Avg_Sales"),
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        count("*").alias("Week_Count")
    )
    
    # Get values
    non_holiday_avg = holiday_kpis.filter(col("Holiday_Flag") == 0) \
                                   .select("Avg_Sales").first()[0]
    holiday_avg = holiday_kpis.filter(col("Holiday_Flag") == 1) \
                               .select("Avg_Sales").first()[0]
    
    # Calculate uplift
    holiday_uplift = ((holiday_avg - non_holiday_avg) / non_holiday_avg) * 100
    
    # Holiday contribution to revenue
    holiday_revenue = holiday_kpis.filter(col("Holiday_Flag") == 1) \
                                   .select("Total_Sales").first()[0]
    total_revenue = holiday_kpis.agg(_sum("Total_Sales")).first()[0]
    holiday_contribution = (holiday_revenue / total_revenue) * 100
    
    metrics = {
        'holiday_uplift_pct': round(holiday_uplift, 2),
        'non_holiday_avg': round(non_holiday_avg, 2),
        'holiday_avg': round(holiday_avg, 2),
        'holiday_revenue_contribution_pct': round(holiday_contribution, 2)
    }
    
    print("\n=== Holiday Impact KPIs ===")
    print(f"Holiday Sales Uplift: {metrics['holiday_uplift_pct']}%")
    print(f"Non-Holiday Avg Sales: ${metrics['non_holiday_avg']:,.2f}")
    print(f"Holiday Avg Sales: ${metrics['holiday_avg']:,.2f}")
    print(f"Holiday Revenue Contribution: {metrics['holiday_revenue_contribution_pct']}%")
    
    return metrics


def calculate_seasonal_kpis(df):
    """
    Calculate seasonal performance KPIs
    
    Args:
        df (DataFrame): Input DataFrame with Season column
        
    Returns:
        DataFrame: Seasonal KPIs
    """
    logger.info("Calculating seasonal KPIs...")
    
    seasonal_kpis = df.groupBy("Season").agg(
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        _round(avg("Weekly_Sales"), 2).alias("Avg_Sales"),
        count("*").alias("Week_Count")
    )
    
    # Calculate season contribution
    total_sales = seasonal_kpis.agg(_sum("Total_Sales")).first()[0]
    seasonal_kpis = seasonal_kpis.withColumn(
        "Revenue_Contribution_Pct",
        _round((col("Total_Sales") / total_sales) * 100, 2)
    )
    
    # Rank seasons
    window_spec = Window.orderBy(desc("Total_Sales"))
    seasonal_kpis = seasonal_kpis.withColumn(
        "Season_Rank",
        rank().over(window_spec)
    )
    
    print("\n=== Seasonal KPIs ===")
    seasonal_kpis.select("Season", "Total_Sales", "Avg_Sales", 
                        "Revenue_Contribution_Pct", "Season_Rank") \
                 .orderBy("Season_Rank").show()
    
    return seasonal_kpis


def calculate_efficiency_metrics(df):
    """
    Calculate operational efficiency metrics
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        DataFrame: Efficiency metrics by store
    """
    logger.info("Calculating efficiency metrics...")
    
    efficiency = df.groupBy("Store").agg(
        _round(avg("Weekly_Sales"), 2).alias("Avg_Sales"),
        _round(_max("Weekly_Sales"), 2).alias("Peak_Sales"),
        _round(_min("Weekly_Sales"), 2).alias("Low_Sales")
    )
    
    # Calculate capacity utilization (assuming peak is capacity)
    efficiency = efficiency.withColumn(
        "Avg_Capacity_Utilization_Pct",
        _round((col("Avg_Sales") / col("Peak_Sales")) * 100, 2)
    )
    
    # Calculate sales consistency
    efficiency = efficiency.withColumn(
        "Sales_Range",
        col("Peak_Sales") - col("Low_Sales")
    )
    
    efficiency = efficiency.withColumn(
        "Consistency_Index",
        _round(100 - ((col("Sales_Range") / col("Avg_Sales")) * 100), 2)
    )
    
    print("\n=== Efficiency Metrics ===")
    efficiency.select("Store", "Avg_Sales", "Peak_Sales", 
                     "Avg_Capacity_Utilization_Pct", "Consistency_Index") \
             .orderBy(desc("Avg_Capacity_Utilization_Pct")).show(10)
    
    return efficiency


def generate_kpi_dashboard(df):
    """
    Generate comprehensive KPI dashboard
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        dict: Complete set of KPIs
    """
    logger.info("Generating KPI dashboard...")
    
    dashboard = {}
    
    # Revenue metrics
    dashboard['revenue'] = calculate_revenue_metrics(df)
    
    # Growth metrics
    dashboard['growth'] = calculate_growth_metrics(df)
    
    # Store performance
    dashboard['store_performance'] = calculate_store_performance_kpis(df)
    
    # Holiday impact
    dashboard['holiday_impact'] = calculate_holiday_impact_kpi(df)
    
    # Seasonal KPIs
    dashboard['seasonal'] = calculate_seasonal_kpis(df)
    
    # Efficiency metrics
    dashboard['efficiency'] = calculate_efficiency_metrics(df)
    
    print("\n" + "="*60)
    print("KPI DASHBOARD GENERATED SUCCESSFULLY")
    print("="*60)
    
    return dashboard


def calculate_top_bottom_performers(df, metric="Total_Sales", n=5):
    """
    Identify top and bottom performing stores
    
    Args:
        df (DataFrame): Input DataFrame
        metric (str): Metric to rank by
        n (int): Number of stores to return
        
    Returns:
        tuple: (top_performers, bottom_performers)
    """
    store_metrics = df.groupBy("Store").agg(
        _round(_sum("Weekly_Sales"), 2).alias("Total_Sales"),
        _round(avg("Weekly_Sales"), 2).alias("Avg_Sales")
    )
    
    top_performers = store_metrics.orderBy(desc(metric)).limit(n)
    bottom_performers = store_metrics.orderBy(asc(metric)).limit(n)
    
    print(f"\n=== Top {n} Performing Stores ===")
    top_performers.show()
    
    print(f"\n=== Bottom {n} Performing Stores ===")
    bottom_performers.show()
    
    return top_performers, bottom_performers
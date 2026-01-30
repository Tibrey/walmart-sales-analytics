"""
Visualization Module
Create comprehensive visualizations for sales analytics using matplotlib and seaborn
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)


def plot_sales_trends(df, store_id=None, save_path=None):
    """
    Plot sales trends over time
    
    Args:
        df (DataFrame): Spark DataFrame
        store_id (int): Optional store ID to filter
        save_path (str): Path to save the plot
    """
    logger.info("Creating sales trends visualization...")
    
    # Filter by store if specified
    if store_id:
        df = df.filter(df.Store == store_id)
    
    # Convert to Pandas for plotting
    df_pd = df.select("Date", "Weekly_Sales").orderBy("Date").toPandas()
    df_pd['Date'] = pd.to_datetime(df_pd['Date'])
    
    plt.figure(figsize=(14, 6))
    plt.plot(df_pd['Date'], df_pd['Weekly_Sales'], linewidth=2, color='#2E86AB')
    
    title = f"Sales Trends Over Time" if not store_id else f"Sales Trends for Store {store_id}"
    plt.title(title, fontsize=16, fontweight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Weekly Sales ($)', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Plot saved to {save_path}")
    
    plt.show()


def plot_store_performance_bar(df, top_n=10, save_path=None):
    """
    Plot top/bottom performing stores
    
    Args:
        df (DataFrame): Spark DataFrame
        top_n (int): Number of stores to show
        save_path (str): Path to save the plot
    """
    logger.info("Creating store performance bar chart...")
    
    # Aggregate by store
    store_sales = df.groupBy("Store").agg(
        {"Weekly_Sales": "sum"}
    ).withColumnRenamed("sum(Weekly_Sales)", "Total_Sales")
    
    # Get top and bottom performers
    top_stores = store_sales.orderBy("Total_Sales", ascending=False).limit(top_n).toPandas()
    bottom_stores = store_sales.orderBy("Total_Sales", ascending=True).limit(top_n).toPandas()
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Top performers
    ax1.barh(top_stores['Store'].astype(str), top_stores['Total_Sales'], color='#06A77D')
    ax1.set_xlabel('Total Sales ($)', fontsize=12)
    ax1.set_ylabel('Store', fontsize=12)
    ax1.set_title(f'Top {top_n} Performing Stores', fontsize=14, fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)
    
    # Bottom performers
    ax2.barh(bottom_stores['Store'].astype(str), bottom_stores['Total_Sales'], color='#D62828')
    ax2.set_xlabel('Total Sales ($)', fontsize=12)
    ax2.set_ylabel('Store', fontsize=12)
    ax2.set_title(f'Bottom {top_n} Performing Stores', fontsize=14, fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Plot saved to {save_path}")
    
    plt.show()


def plot_seasonal_patterns(df, save_path=None):
    """
    Plot seasonal sales patterns
    
    Args:
        df (DataFrame): Spark DataFrame with Season column
        save_path (str): Path to save the plot
    """
    logger.info("Creating seasonal patterns visualization...")
    
    seasonal_data = df.groupBy("Season").agg(
        {"Weekly_Sales": "avg"}
    ).withColumnRenamed("avg(Weekly_Sales)", "Avg_Sales").toPandas()
    
    # Order seasons
    season_order = ['Winter', 'Spring', 'Summer', 'Fall']
    seasonal_data['Season'] = pd.Categorical(seasonal_data['Season'], 
                                             categories=season_order, 
                                             ordered=True)
    seasonal_data = seasonal_data.sort_values('Season')
    
    plt.figure(figsize=(10, 6))
    colors = ['#A8DADC', '#457B9D', '#F1FAEE', '#E63946']
    bars = plt.bar(seasonal_data['Season'], seasonal_data['Avg_Sales'], 
                   color=colors, edgecolor='black', linewidth=1.5)
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'${height:,.0f}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.title('Average Sales by Season', fontsize=16, fontweight='bold')
    plt.xlabel('Season', fontsize=12)
    plt.ylabel('Average Weekly Sales ($)', fontsize=12)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Plot saved to {save_path}")
    
    plt.show()


def plot_monthly_trends(df, save_path=None):
    """
    Plot monthly sales trends
    
    Args:
        df (DataFrame): Spark DataFrame with Month column
        save_path (str): Path to save the plot
    """
    logger.info("Creating monthly trends visualization...")
    
    monthly_data = df.groupBy("Month", "MonthName").agg(
        {"Weekly_Sales": "avg"}
    ).withColumnRenamed("avg(Weekly_Sales)", "Avg_Sales") \
     .orderBy("Month").toPandas()
    
    plt.figure(figsize=(14, 6))
    plt.plot(monthly_data['MonthName'], monthly_data['Avg_Sales'], 
            marker='o', linewidth=2, markersize=8, color='#F77F00')
    plt.fill_between(range(len(monthly_data)), monthly_data['Avg_Sales'], 
                     alpha=0.3, color='#F77F00')
    
    plt.title('Average Sales by Month', fontsize=16, fontweight='bold')
    plt.xlabel('Month', fontsize=12)
    plt.ylabel('Average Weekly Sales ($)', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Plot saved to {save_path}")
    
    plt.show()


def plot_holiday_impact(df, save_path=None):
    """
    Plot holiday vs non-holiday sales comparison
    
    Args:
        df (DataFrame): Spark DataFrame with Holiday_Flag
        save_path (str): Path to save the plot
    """
    logger.info("Creating holiday impact visualization...")
    
    holiday_data = df.groupBy("Holiday_Flag").agg(
        {"Weekly_Sales": "avg"}
    ).withColumnRenamed("avg(Weekly_Sales)", "Avg_Sales").toPandas()
    
    holiday_data['Period'] = holiday_data['Holiday_Flag'].map({0: 'Non-Holiday', 1: 'Holiday'})
    
    plt.figure(figsize=(10, 6))
    colors = ['#4A4E69', '#9A8C98']
    bars = plt.bar(holiday_data['Period'], holiday_data['Avg_Sales'], 
                   color=colors, edgecolor='black', linewidth=1.5)
    
    # Add value labels and percentage difference
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'${height:,.0f}',
                ha='center', va='bottom', fontsize=12, fontweight='bold')
    
    # Calculate uplift
    non_holiday = holiday_data[holiday_data['Period'] == 'Non-Holiday']['Avg_Sales'].values[0]
    holiday = holiday_data[holiday_data['Period'] == 'Holiday']['Avg_Sales'].values[0]
    uplift = ((holiday - non_holiday) / non_holiday) * 100
    
    plt.title(f'Holiday Impact on Sales (Uplift: {uplift:.1f}%)', 
             fontsize=16, fontweight='bold')
    plt.xlabel('Period Type', fontsize=12)
    plt.ylabel('Average Weekly Sales ($)', fontsize=12)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Plot saved to {save_path}")
    
    plt.show()


def plot_sales_distribution(df, save_path=None):
    """
    Plot distribution of weekly sales
    
    Args:
        df (DataFrame): Spark DataFrame
        save_path (str): Path to save the plot
    """
    logger.info("Creating sales distribution plot...")
    
    sales_data = df.select("Weekly_Sales").toPandas()
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Histogram
    ax1.hist(sales_data['Weekly_Sales'], bins=50, color='#2A9D8F', 
            edgecolor='black', alpha=0.7)
    ax1.set_xlabel('Weekly Sales ($)', fontsize=12)
    ax1.set_ylabel('Frequency', fontsize=12)
    ax1.set_title('Sales Distribution', fontsize=14, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    
    # Box plot
    ax2.boxplot(sales_data['Weekly_Sales'], vert=True, patch_artist=True,
               boxprops=dict(facecolor='#E76F51', alpha=0.7),
               medianprops=dict(color='black', linewidth=2))
    ax2.set_ylabel('Weekly Sales ($)', fontsize=12)
    ax2.set_title('Sales Box Plot', fontsize=14, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Plot saved to {save_path}")
    
    plt.show()


def plot_correlation_heatmap(df, save_path=None):
    """
    Plot correlation heatmap for numeric variables
    
    Args:
        df (DataFrame): Spark DataFrame
        save_path (str): Path to save the plot
    """
    logger.info("Creating correlation heatmap...")
    
    # Select numeric columns
    numeric_cols = ["Weekly_Sales", "Temperature", "Fuel_Price", "CPI", "Unemployment"]
    df_pd = df.select(numeric_cols).toPandas()
    
    # Calculate correlation matrix
    corr_matrix = df_pd.corr()
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=True, fmt='.3f', cmap='coolwarm', 
               center=0, square=True, linewidths=1, cbar_kws={"shrink": 0.8})
    
    plt.title('Correlation Heatmap: Sales vs External Factors', 
             fontsize=16, fontweight='bold')
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Plot saved to {save_path}")
    
    plt.show()


def plot_yoy_comparison(df, save_path=None):
    """
    Plot year-over-year sales comparison
    
    Args:
        df (DataFrame): Spark DataFrame with Year column
        save_path (str): Path to save the plot
    """
    logger.info("Creating year-over-year comparison plot...")
    
    yearly_data = df.groupBy("Year").agg(
        {"Weekly_Sales": "sum"}
    ).withColumnRenamed("sum(Weekly_Sales)", "Total_Sales") \
     .orderBy("Year").toPandas()
    
    plt.figure(figsize=(12, 6))
    bars = plt.bar(yearly_data['Year'].astype(str), yearly_data['Total_Sales'],
                  color='#264653', edgecolor='black', linewidth=1.5)
    
    # Add value labels
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'${height:,.0f}',
                ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    plt.title('Year-over-Year Sales Comparison', fontsize=16, fontweight='bold')
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Total Sales ($)', fontsize=12)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Plot saved to {save_path}")
    
    plt.show()


def create_visualization_dashboard(df, output_dir="../outputs/visualizations/"):
    """
    Create a complete set of visualizations
    
    Args:
        df (DataFrame): Spark DataFrame with all features
        output_dir (str): Directory to save visualizations
    """
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info("Creating comprehensive visualization dashboard...")
    
    # 1. Sales trends
    plot_sales_trends(df, save_path=f"{output_dir}sales_trends.png")
    
    # 2. Store performance
    plot_store_performance_bar(df, top_n=10, save_path=f"{output_dir}store_performance.png")
    
    # 3. Seasonal patterns
    plot_seasonal_patterns(df, save_path=f"{output_dir}seasonal_patterns.png")
    
    # 4. Monthly trends
    plot_monthly_trends(df, save_path=f"{output_dir}monthly_trends.png")
    
    # 5. Holiday impact
    plot_holiday_impact(df, save_path=f"{output_dir}holiday_impact.png")
    
    # 6. Sales distribution
    plot_sales_distribution(df, save_path=f"{output_dir}sales_distribution.png")
    
    # 7. Correlation heatmap
    plot_correlation_heatmap(df, save_path=f"{output_dir}correlation_heatmap.png")
    
    # 8. YoY comparison
    plot_yoy_comparison(df, save_path=f"{output_dir}yoy_comparison.png")
    
    logger.info(f"All visualizations saved to {output_dir}")
    print(f"\n✓ Visualization dashboard created successfully!")
    print(f"  Output directory: {output_dir}")
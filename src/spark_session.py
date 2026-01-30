from pyspark.sql import SparkSession
from pyspark import SparkConf
import logging

# Configure Logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name = "WalmartSalesAnalytics",
                         master="local[*]",
                         memory="4g"):
    """
    Create and configure a SparkSession with optimized settings foe sales analytics

    Args:
        app_name(str): Name of the Spark application
        master(str): Spark master URL (default: local[*] uses all available cores)
        memory(str): Driver memory allocation
    
        Returns:
        SparkSession: Configured Spark session
    """

    try:
        # Configure Spark settings
        conf = SparkConf()
        conf.set("spark.driver.memory", memory)
        conf.set("spark.sql.shuffle.partitions","100")
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled","true")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Create spark session
        spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config(conf=conf) \
            .getOrCreate()
        
        # Set log level to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")

        logger.info(f"SparkSession created successfully: {app_name}")
        logger.info(f"Spark Version: {spark.version}")
        logger.info(f"Master: {master}")

        return spark
    
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {str(e)}")
        raise

def stop_spark_session(spark):
    """
    Stop the spark session

    Aargs:
        spark(SparkSession): Active Spark session to stop
    """
    try:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped successfully")
    except Exception as e:
        logger.error(f"Error stopping the SparkSession: {str(e)}")
    

# Example usage
# if __name__ == "__main__":

#     spark = create_spark_session()

#     # Display Spark configuration
#     print("\n=== Spark Configuration ===")
#     print(f"App Name: {spark.sparkContext.appName}")
#     print(f"Spark Version: {spark.version}")
#     print(f"Master: {spark.sparkContext.master}")
#     print(f"Available Cores: {spark.sparkContext.defaultParallelism}")
    
#     # Stop session
#     stop_spark_session(spark)

import os
import logging
import platform
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, hour, when, concat_ws, lit
from pyspark.sql.types import IntegerType
from pyspark.sql import DataFrame
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(processName)s] - %(message)s")
logger = logging.getLogger(__name__)

def get_spark_session() -> SparkSession:
    system = platform.system().lower()
    temp_dir = str(Path(f"/tmp/spark-{os.getpid()}")) if system != 'windows' else str(Path(os.getenv('TEMP', 'C:\\Temp')) / f"spark-{os.getpid()}")
    try:
        spark = SparkSession.builder \
            .appName(f"NYCTaxiETLLocal-{os.getpid()}") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.default.parallelism", "2") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.ui.enabled", "false") \
            .config("spark.driver.host", "localhost") \
            .config("spark.local.dir", temp_dir) \
            .master("local[2]") \
            .getOrCreate()
        logger.debug("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def extract(spark: SparkSession, input_path: str) -> DataFrame:
    input_path = str(Path(input_path))
    logger.debug(f"Attempting to read Parquet file from: {input_path}")
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        raise FileNotFoundError(f"Input file not found: {input_path}")
    try:
        df = spark.read.parquet(input_path)
        row_count = df.count()
        logger.info(f"Chunk loaded successfully with {row_count} rows.")
        return df
    except Exception as e:
        logger.error(f"Failed to read Parquet file: {e}")
        raise

def transform(df: DataFrame) -> DataFrame:
    try:
        logger.debug("Starting data transformation...")
        df = df.na.drop()
        df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
        df = df.withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
        df = df.withColumn(
            "trip_duration",
            (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
        )
        df = df.withColumn(
            "speed_mph",
            when(col("trip_duration") != 0, col("trip_distance") / (col("trip_duration") / 60)).otherwise(None)
        )
        df = df.withColumn(
            "is_airport_trip",
            col("PULocationID").isin([1, 2, 3]) | col("DOLocationID").isin([1, 2, 3])
        )
        df = df.withColumn(
            "tip_percentage",
            when(col("fare_amount") != 0, (col("tip_amount") / col("fare_amount")) * 100).otherwise(None)
        )
        df = df.withColumn(
            "cost_per_mile",
            when(col("trip_distance") != 0, col("total_amount") / col("trip_distance")).otherwise(None)
        )
        df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        df = df.withColumn(
            "is_peak_hour",
            when((col("pickup_hour").between(7, 9)) | (col("pickup_hour").between(17, 19)), True).otherwise(False)
        )
        df = df.withColumn(
            "trip_summary",
            concat_ws(
                " ",
                lit("Trip from location"),
                col("PULocationID").cast("string"),
                lit("to"),
                col("DOLocationID").cast("string"),
                lit("with"),
                col("passenger_count").cast(IntegerType()).cast("string"),
                lit("passenger(s), covering"),
                col("trip_distance").cast("string"),
                lit("miles in"),
                col("trip_duration").cast("string"),
                lit("minutes.")
            )
        )
        row_count = df.count()
        logger.debug(f"Transformation completed. Output rows: {row_count}")
        return df
    except Exception as e:
        logger.error(f"Transformation error: {e}")
        raise

def load(df: DataFrame, output_path: str) -> None:
    output_path = str(Path(output_path))
    logger.debug(f"Writing results to: {output_path}")
    try:
        result_df = df.toPandas()
        result_str = result_df.to_string(index=False)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            f.write("\n--- Chunk Results ---\n")
            f.write(result_str)
            f.write("\n")
        logger.info(f"Results written to {output_path}")
    except Exception as e:
        logger.error(f"Failed to write results: {e}")
        raise

def run(input_path: str, task_id: str, output_dir: str) -> None:
    logger.info("Starting ETL task...")
    spark = None
    try:
        spark = get_spark_session()
        output_path = os.path.join(output_dir, f"chunk_result_{task_id}.txt")
        logger.info(f"Input path: {input_path}")
        logger.info(f"Output path: {output_path}")
        df = extract(spark, input_path)
        df = transform(df)
        load(df, output_path)
        logger.info("ETL task completed.")
    except Exception as e:
        logger.error(f"ETL task failed: {e}")
        raise
    finally:
        if spark is not None:
            try:
                spark.sparkContext.cancelAllJobs()
                spark.stop()
                logger.debug("Spark session stopped.")
            except Exception as e:
                logger.error(f"Failed to stop Spark session: {e}")

if __name__ == "__main__":
    run(
        input_path=os.getenv("INPUT_PATH", "../../source_dir/data.bin"),
        task_id=os.getenv("TASK_ID", "0"),
        output_dir=os.getenv("OUTPUT_PATH", "../../task_results_dir")
    )
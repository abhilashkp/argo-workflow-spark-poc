import argparse
import os
from pyspark.sql import SparkSession

# Read from environment variables (with defaults)
APP_NAME = os.getenv("APP_NAME", "default-spark-app")
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT", "bmdatalaketest")
CONTAINER = os.getenv("CONTAINER", "mahesh")
WAREHOUSE = os.getenv("WAREHOUSE", "default")

# Hardcoded values (for non-strategy parameters)
CATALOG = "spark_catalog"
DATABASE = "smallfiles"
TABLE = "uncompacted_2705_new"
TARGET_FILE_SIZE_BYTES = 10 * 1024 * 1024 # 10 MB
MIN_INPUT_FILES = 2
REWRITE_ALL = True
MAX_CONCURRENT_GROUP_REWRITES = 2
PARTIAL_PROGRESS_ENABLED = True
PARTIAL_PROGRESS_MAX_COMMITS = 5

# Columns available for sort strategy (case-insensitive)
VALID_SORT_COLUMNS = {
    "diabetes_binary", "highbp", "highchol", "cholcheck", "bmi", "smoker", "stroke",
    "heartdiseaseorattack", "physactivity", "fruits", "veggies", "hvyalcoholconsump",
    "anyhealthcare", "nodocbccost", "genhlth", "menthlth", "physhlth", "diffwalk",
    "sex", "age", "education", "income"
}

def configure_spark():
    return SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop") \
        .config(f"spark.sql.catalog.{CATALOG}.warehouse",
                f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{WAREHOUSE}") \
        .getOrCreate()

def configure_azure_access(spark, sas_token):
    spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{STORAGE_ACCOUNT}.dfs.core.windows.net", sas_token.lstrip("?"))

def validate_sort_columns(sort_order):
    """Ensure all columns in sort_order are valid for the dataset."""
    columns = [col.strip().split()[0].lower() for col in sort_order.split(",")]
    invalid = [col for col in columns if col not in VALID_SORT_COLUMNS]
    if invalid:
        raise ValueError(f"Invalid sort columns: {invalid}. Valid options: {sorted(VALID_SORT_COLUMNS)}")
    return sort_order

def run_compaction(spark, strategy, sort_order=None):
    options = [
        f"'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}'",
        f"'min-input-files', '{MIN_INPUT_FILES}'",
        f"'rewrite-all', '{str(REWRITE_ALL).lower()}'",
        f"'max-concurrent-file-group-rewrites', '{MAX_CONCURRENT_GROUP_REWRITES}'",
        f"'partial-progress.enabled', '{str(PARTIAL_PROGRESS_ENABLED).lower()}'",
        f"'partial-progress.max-commits', '{PARTIAL_PROGRESS_MAX_COMMITS}'"
    ]
    strategy_clause = ""
    if strategy == 'sort':
        if not sort_order:
            raise ValueError("Sort order must be specified for sort strategy")
        validate_sort_columns(sort_order)
        strategy_clause = f"sort_order => '{sort_order}',"
    compaction_query = f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table => '{DATABASE}.{TABLE}',
            strategy => '{strategy}',
            {strategy_clause}
            options => map({', '.join(options)})
        )
    """
    print("Executing compaction query:")
    print(compaction_query)
    result = spark.sql(compaction_query)
    result.show(truncate=False)

def main():
    parser = argparse.ArgumentParser(description='Iceberg Compaction: SAS token and strategy parameters')
    parser.add_argument('--sas-token', required=True, help='SAS token for ADLS access')
    parser.add_argument('--strategy', choices=['binpack', 'sort'], default='binpack',
                       help='Compaction strategy (default: binpack)')
    parser.add_argument('--sort-order', help='Sort column(s) for sort strategy (required if using sort)')
    args = parser.parse_args()

    if args.strategy == 'sort' and not args.sort_order:
        parser.error("--sort-order is required when using 'sort' strategy")

    spark = configure_spark()
    try:
        configure_azure_access(spark, args.sas_token)
        print(f"Starting {args.strategy} compaction process with app name: {APP_NAME}")
        run_compaction(spark, args.strategy, args.sort_order)
        print("Compaction completed successfully")
    except Exception as e:
        print(f"Compaction failed: {str(e)}")
        raise
    finally:
        spark.stop()
        print("Spark session closed")

if __name__ == "__main__":
    main()

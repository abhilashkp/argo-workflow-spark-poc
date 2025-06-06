import argparse
import json
import logging
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse
from pyspark.sql import SparkSession

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Environment variables
APP_NAME = os.getenv("APP_NAME", "iceberg-maintenance")
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT", "bmdatalaketest")
CONTAINER = os.getenv("CONTAINER", "mahesh")

def configure_azure_access(spark, sas_token):
    """Configure Azure access for Spark."""
    spark.conf.set(
        f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", 
        "SAS"
    )
    spark.conf.set(
        f"fs.azure.sas.token.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
    )
    spark.conf.set(
        f"fs.azure.sas.fixed.token.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        sas_token
    )

def extract_db_and_table(location):
    """Extract database and table name from ABFS or HTTPS path."""
    if location.startswith("https://"):
        location = location.replace("https://", "abfss://", 1)
    parsed = urlparse(location)
    path_parts = parsed.path.strip('/').split('/')
    if len(path_parts) < 2:
        raise ValueError(f"Invalid path: {location}")
    return path_parts[-2], path_parts[-1]

def run_compaction(spark, table_ref, compaction_config):
    """Run compaction with valid size parameters."""
    strategy = compaction_config.get("strategy", "binpack")
    max_file_size = compaction_config.get("maxfilesizebytes", 134217728)  # 128MB
    target_file_size = max_file_size - 1048576  # 1MB buffer

    options = {
        'target-file-size-bytes': str(target_file_size),
        'max-file-size-bytes': str(max_file_size),
        'min-input-files': str(compaction_config.get("filecountthreshold", 50)),
        'max-concurrent-file-group-rewrites': str(compaction_config.get("maxparalleltasks", 10)),
        'rewrite-all': 'true'
    }
    
    options_str = ",\n".join([f"'{k}', '{v}'" for k, v in options.items()])
    
    sql = f"""
        CALL {table_ref['catalog']}.system.rewrite_data_files(
            table => '{table_ref['database']}.{table_ref['table']}',
            strategy => '{strategy}',
            options => map({options_str})
        )
    """
    logger.info(f"Compacting {table_ref['database']}.{table_ref['table']}")
    spark.sql(sql)

def expire_snapshots(spark, table_name, location, snapshot_expiration_days):
    """Expires snapshots based on retention days; -1 removes all except the latest."""
    try:
        db_name, tbl_name = extract_db_and_table(location)
        full_table_name = f"{db_name}.{tbl_name}"

        if snapshot_expiration_days < 0:
            # Set cutoff to now: remove all snapshots older than the current time
            cutoff_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            cutoff_timestamp = (
                datetime.now() - timedelta(days=snapshot_expiration_days)
            ).strftime("%Y-%m-%d %H:%M:%S")

        spark.sql(f"""
            CALL iceberg.system.expire_snapshots(
                table => '{full_table_name}',
                older_than => TIMESTAMP '{cutoff_timestamp}'
            )
        """)
        logger.info(f"Snapshot expiration completed for {full_table_name}")
        return True
    except Exception as e:
        logger.error(f"Snapshot expiration failed for {table_name}: {str(e)}")
        return False

def process_snapshot_job(spark, config):
    """Handles snapshot operations for multiple data products"""
    results = {"success": [], "failed": []}
    
    # If config is not a list, wrap it in a list for robustness
    data_products = config if isinstance(config, list) else [config]
    
    for dp in data_products:
        data_product_name = dp.get("data_product", "unknown_product")
        tables = dp.get("iceberg_tables", [])
        for table_config in tables:
            table_name = table_config.get("table_name", "unnamed_table")
            if "snapshot_expiration_days" not in table_config:
                logger.warning(f"Skipping {data_product_name}.{table_name} - no expiration policy")
                results["failed"].append(f"{data_product_name}.{table_name}")
                continue

            success = expire_snapshots(
                spark,
                table_name,
                table_config["location"],
                table_config["snapshot_expiration_days"]
            )
            
            if success:
                results["success"].append(f"{data_product_name}.{table_name}")
            else:
                results["failed"].append(f"{data_product_name}.{table_name}")
    
    return results

def remove_orphan_files(spark, table_ref, retention_days):
    """Remove orphan files using Iceberg's remove_orphan_files."""
    older_than = datetime.now() - timedelta(days=retention_days)
    sql = f"""
        CALL {table_ref['catalog']}.system.remove_orphan_files(
            table => '{table_ref['database']}.{table_ref['table']}',
            older_than => TIMESTAMP '{older_than.strftime('%Y-%m-%d %H:%M:%S')}'
        )
    """
    logger.info(f"Removing orphan files older than {retention_days} days")
    spark.sql(sql)
    return "success"

def process_compaction(spark, config):
    """Process compaction configuration."""
    try:
        default_location = "https://bmdatalaketest.dfs.core.windows.net/mahesh/data_product/table1/"
        location = config.get("location", default_location)
        db, table = extract_db_and_table(location)
        table_ref = {
            "catalog": "iceberg",
            "database": db,
            "table": table
        }
        run_compaction(spark, table_ref, config)
        return {"status": "success", "table": f"{db}.{table}"}
    except Exception as e:
        logger.error(f"Compaction failed: {str(e)}")
        return {"status": "failed", "error": str(e)}


WAREHOUSE = os.getenv("WAREHOUSE", "default")
def main():
    parser = argparse.ArgumentParser(description='Iceberg Table Maintenance')
    parser.add_argument('--sas-token', required=True, help='SAS token for ADLS access')
    parser.add_argument('--config', required=True, help='JSON config string')
    parser.add_argument('--job', choices=['compaction','snapshot'], required=True)
#    parser.add_argument('--output', default="results.json", help='Output file path')
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/") \
        .config("spark.jars.packages", ",".join([
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0",
            "org.apache.hadoop:hadoop-azure:3.3.1"
        ])) \
        .getOrCreate()
    configure_azure_access(spark, args.sas_token)

    try:
        config = json.loads(args.config)
        results = []

        if args.job == "compaction":
            results.append(process_compaction(spark, config))
        elif args.job == "snapshot":
            results = process_snapshot_job(spark, config)

#        with open(args.output, "w") as f:
#            json.dump(results, f, indent=2)
#        logger.info(f"Results saved to {args.output}")

    except Exception as e:
        logger.error(f"Main process failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

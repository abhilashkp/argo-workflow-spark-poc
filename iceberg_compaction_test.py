import argparse
import json
import os
import sys
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
 
# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
 
# Environment variables (with defaults)
APP_NAME = os.getenv("APP_NAME", "iceberg-maintenance")
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT", "bmdatalaketest")
CONTAINER = os.getenv("CONTAINER", "mahesh")
WAREHOUSE = os.getenv("WAREHOUSE", "default")
 
def configure_spark(catalog):
    """Create Spark session with catalog-specific configuration"""
    logging.info(f"Configuring Spark session for catalog: {catalog}")
    return SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop") \
        .config(f"spark.sql.catalog.{catalog}.warehouse",
                f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{WAREHOUSE}") \
        .getOrCreate()
 
def configure_azure_access(spark, sas_token):
    """Configure Azure storage access for all operations"""
    logging.info("Configuring Azure ADLS access for Spark")
    spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{STORAGE_ACCOUNT}.dfs.core.windows.net", sas_token.lstrip("?"))
 
def run_compaction(spark, table_ref, compaction_conf):
    """Execute compaction using data product-level configuration"""
    # All required fields must be present in config
    strategy = compaction_conf["strategy"]
    sort_order = compaction_conf.get("sort_order")
    target_file_size_bytes = compaction_conf["target_file_size_bytes"]
    min_input_files = compaction_conf["min_input_files"]
    rewrite_all = compaction_conf["rewrite_all"]
    max_concurrent_group_rewrites = compaction_conf["max_concurrent_group_rewrites"]
 
    options = [
        f"'target-file-size-bytes', '{target_file_size_bytes}'",
        f"'min-input-files', '{min_input_files}'",
        f"'rewrite-all', '{str(rewrite_all).lower()}'",
        f"'max-concurrent-file-group-rewrites', '{max_concurrent_group_rewrites}'"
    ]
    # Add optional partial progress options if present
    if "partial_progress_enabled" in compaction_conf:
        options.append(f"'partial-progress.enabled', '{str(compaction_conf['partial_progress_enabled']).lower()}'")
    if "partial_progress_max_commits" in compaction_conf:
        options.append(f"'partial-progress.max-commits', '{compaction_conf['partial_progress_max_commits']}'")
 
    strategy_clause = ""
    if strategy == "sort" and sort_order:
        strategy_clause = f"sort_order => '{sort_order}',"
 
    query = f"""
        CALL {table_ref['catalog']}.system.rewrite_data_files(
            table => '{table_ref['database']}.{table_ref['table']}',
            strategy => '{strategy}',
            {strategy_clause}
            options => map({', '.join(options)})
        )
    """
    logging.info(f"Compacting {table_ref['catalog']}.{table_ref['database']}.{table_ref['table']}")
    logging.debug(query)
    spark.sql(query).show(truncate=False)
 
def expire_snapshots(spark, table_ref, snapshot_conf):
    """Expire snapshots based on table-specific configuration"""
    retention_days = snapshot_conf["retention_days"]
    retain_last = snapshot_conf["retain_last"]
 
    # Validate retain_last to prevent Iceberg errors
    if retain_last < 1:
        raise ValueError("retain_last must be at least 1")
    if not isinstance(retention_days, int):
        raise TypeError("retention_days must be an integer")
 
    cutoff_time = datetime.now() - timedelta(days=retention_days)
    cutoff_timestamp = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")
 
    query = f"""
        CALL {table_ref['catalog']}.system.expire_snapshots(
            table => '{table_ref['database']}.{table_ref['table']}',
            older_than => TIMESTAMP '{cutoff_timestamp}',
            retain_last => {retain_last}
        )
    """
 
    logging.info(f"Expiring snapshots older than {cutoff_timestamp} (retain_last={retain_last}) for {table_ref['catalog']}.{table_ref['database']}.{table_ref['table']}")
 
    try:
        result = spark.sql(query)
        if result.rdd.isEmpty():
            logging.info("No snapshots matched expiration criteria")
            return 0
        deleted_files = result.select("deleted_data_files").first()[0]
        expired_snapshots = result.select("expired_snapshot_id").collect()
        logging.info(f"Expired {len(expired_snapshots)} snapshots and {deleted_files} data files")
        result.show(truncate=False)
        return deleted_files
    except Exception as e:
        logging.error(f"Failed to expire snapshots: {str(e)}")
        raise
 
def remove_orphan_files(spark, table_ref, orphan_conf):
    retention_days = orphan_conf["retention_days"]
    if not isinstance(retention_days, int):
        raise TypeError("retention_days must be an integer")
    cutoff_time = datetime.now() - timedelta(days=retention_days)
    cutoff_timestamp = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
        CALL {table_ref['catalog']}.system.remove_orphan_files(
            table => '{table_ref['database']}.{table_ref['table']}',
            older_than => TIMESTAMP '{cutoff_timestamp}'
        )
    """
    logging.info(f"Cleaning orphans for {table_ref['catalog']}.{table_ref['database']}.{table_ref['table']}")
    logging.debug(query)
    spark.sql(query).show(truncate=False)
 
def process_table(spark, sas_token, data_product_conf, table_conf, job):
    try:
        configure_azure_access(spark, sas_token)
        table_ref = {
            "catalog": table_conf["catalog"],
            "database": table_conf["database"],
            "table": table_conf["table"]
        }
        maintenance = table_conf.get("maintenance", {})
        if job == "compaction":
            run_compaction(spark, table_ref, data_product_conf["compaction"])
        elif job == "snapshot":
            if "snapshot_expiration" in maintenance:
                expire_snapshots(spark, table_ref, maintenance["snapshot_expiration"])
            else:
                logging.info(f"Snapshot expiration not configured for {table_ref['catalog']}.{table_ref['database']}.{table_ref['table']}")
        elif job == "orphan":
            if "orphan_cleanup" in maintenance:
                remove_orphan_files(spark, table_ref, maintenance["orphan_cleanup"])
            else:
                logging.info(f"Orphan cleanup not configured for {table_ref['catalog']}.{table_ref['database']}.{table_ref['table']}")
    except Exception as e:
        logging.error(f"Error processing {table_conf['catalog']}.{table_conf['database']}.{table_conf['table']}: {str(e)}", exc_info=True)
 
def validate_config(data_product_conf):
    # Data product level
    for field in ["name", "compaction", "tables"]:
        if field not in data_product_conf:
            raise ValueError(f"Missing '{field}' in data product config.")
 
    # Compaction required fields
    for field in ["strategy", "target_file_size_bytes", "min_input_files", "rewrite_all", "max_concurrent_group_rewrites"]:
        if field not in data_product_conf["compaction"]:
            raise ValueError(f"Missing '{field}' in compaction config.")
 
    # Tables and maintenance
    for table in data_product_conf["tables"]:
        for field in ["catalog", "database", "table", "maintenance"]:
            if field not in table:
                raise ValueError(f"Missing '{field}' in table config.")
        maintenance = table["maintenance"]
        if "snapshot_expiration" in maintenance:
            for field in ["retention_days", "retain_last"]:
                if field not in maintenance["snapshot_expiration"]:
                    raise ValueError(f"Missing '{field}' in snapshot_expiration config.")
        if "orphan_cleanup" in maintenance:
            if "retention_days" not in maintenance["orphan_cleanup"]:
                raise ValueError("Missing 'retention_days' in orphan_cleanup config.")
 
def main():
    parser = argparse.ArgumentParser(description='Iceberg Maintenance for Single Data Product')
    parser.add_argument('--sas-token', required=True, help='SAS token for ADLS access')
    parser.add_argument('--config', required=True, help='Path to data product config file')
    parser.add_argument('--job', choices=['compaction', 'snapshot', 'orphan'], required=True,
                        help='Maintenance job to run: compaction, snapshot, or orphan')
    args = parser.parse_args()
 
    try:
        with open(args.config, 'r') as f:
            data_product_conf = json.load(f)
        validate_config(data_product_conf)
    except Exception as e:
        logging.error(f"Error loading or validating config file: {str(e)}")
        sys.exit(1)
 
    logging.info(f"Processing data product: {data_product_conf.get('name', 'unknown')}")
    for table in data_product_conf["tables"]:
        logging.info(f"  Table: {table['catalog']}.{table['database']}.{table['table']}")
        spark = configure_spark(table["catalog"])
        try:
            process_table(spark, args.sas_token, data_product_conf, table, args.job)
        finally:
            spark.stop()
            logging.info("    Spark session closed\n")
 
if __name__ == "__main__":
    main()
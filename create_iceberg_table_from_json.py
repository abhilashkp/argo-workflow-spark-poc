import json
import argparse
from pyspark.sql import SparkSession
from urllib.parse import urlparse

def create_iceberg_table_from_json_string(json_str, sas_token=None):
    # Parse the JSON string
    conf = json.loads(json_str)

    catalog = conf["catalog"]
    warehouse = catalog["warehouse"]
    catalog_name = catalog.get("name", "spark_catalog")
    catalog_type = catalog.get("type", "hadoop")

    database = conf["database"]
    table = conf["table"]
    full_table_name = f"{catalog_name}.{database}.{table}"  # Flatten namespace

    # location = conf["location"]
    location = f"{warehouse}/{database}/{table}"
    schema_fields = conf["schema"]

    # Build schema string
    schema_str = ",\n  ".join([f'{field["name"]} {field["type"].upper()}' for field in schema_fields])

    # Start Spark session
    spark_builder = (
        SparkSession.builder
        .appName("CreateIcebergTableFromJSONString")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", catalog_type)
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
    )
    
    # Configure ADLS access if SAS token is provided
    if sas_token:
        # Parse warehouse URL to get storage account
        parsed_warehouse = urlparse(warehouse)
        if parsed_warehouse.scheme in ['abfss', 'abfs']:
            # Extract storage account from the URL
            # URL format: abfss://container@storageaccount.dfs.core.windows.net/path
            netloc_parts = parsed_warehouse.netloc.split('@')
            if len(netloc_parts) == 2:
                container = netloc_parts[0]
                storage_account_domain = netloc_parts[1]
                storage_account = storage_account_domain.split('.')[0]
                
                # Configure SAS token authentication
                spark_builder = spark_builder.config(
                    f"spark.hadoop.fs.azure.sas.{container}.{storage_account}.dfs.core.windows.net", 
                    sas_token
                )
                print(f"🔐 Configured ADLS SAS token for container '{container}' in storage account '{storage_account}'")
            else:
                print("⚠️  Warning: Could not parse storage account from warehouse URL")
        else:
            print("⚠️  Warning: SAS token provided but warehouse is not an ADLS URL")
    
    spark = spark_builder.getOrCreate()

    # Create table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
          {schema_str}
        )
        USING iceberg
        LOCATION '{location}'
    """)

    print(f"✅ Iceberg table {full_table_name} created at {location}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Iceberg table from JSON string")
    parser.add_argument("--json", required=True, help="JSON string with table definition")
    parser.add_argument("--sas-token", help="SAS token for ADLS authentication")
    args = parser.parse_args()

    create_iceberg_table_from_json_string(args.json, args.sas_token)

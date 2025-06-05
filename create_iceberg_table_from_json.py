import json
import argparse
from pyspark.sql import SparkSession

def create_iceberg_table_from_json_string(json_str):
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
    spark = (
        SparkSession.builder
        .appName("CreateIcebergTableFromJSONString")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", catalog_type)
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
        .getOrCreate()
    )

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
    args = parser.parse_args()

    create_iceberg_table_from_json_string(args.json)

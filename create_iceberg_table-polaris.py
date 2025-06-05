import json
import argparse
from pyspark.sql import SparkSession

def create_iceberg_table_from_json(json_str):
    # Parse the JSON string
    conf = json.loads(json_str)

    catalog = conf["catalog"]
    warehouse = catalog["warehouse"]
    catalog_name = catalog.get("name", "spark_catalog")
    catalog_type = catalog.get("type", "rest")
    uri = catalog.get("uri")  # Polaris REST URI (important for Polaris)

    database = conf["database"]
    table = conf["table"]
    full_table_name = f"{catalog_name}.{database}.{table}"

    location = conf["location"]
    schema_fields = conf["schema"]

    schema_str = ",\n  ".join([f'{field["name"]} {field["type"].upper()}' for field in schema_fields])

    builder = (
        SparkSession.builder
        .appName("CreateIcebergTableWithPolaris")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", catalog_type)
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
    )

    if uri:
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.uri", uri)

    spark = builder.getOrCreate()

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
    parser = argparse.ArgumentParser(description="Create Iceberg table using Polaris catalog from JSON string")
    parser.add_argument("--json", required=True, help="JSON string with table configuration")
    args = parser.parse_args()

    create_iceberg_table_from_json(args.json)

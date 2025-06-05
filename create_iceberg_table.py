import argparse
from pyspark.sql import SparkSession

def create_iceberg_table(warehouse, database, table):
    # Construct full table name and location
    full_table_name = f"spark_catalog.{database}.{table}"
    table_location = f"{warehouse}/{database}/{table}"

    # Start SparkSession with Iceberg and Hadoop catalog config
    spark = (
        SparkSession.builder
        .appName("CreateIcebergTable")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", warehouse)
        .getOrCreate()
    )

    # Create the table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            id INT,
            name STRING,
            ts TIMESTAMP
        )
        USING iceberg
        LOCATION '{table_location}'
    """)

    print(f"✅ Table {full_table_name} created at {table_location}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Iceberg table using Hadoop catalog")
    parser.add_argument("--warehouse", required=True, help="Iceberg warehouse path (e.g. abfs://...)")
    parser.add_argument("--database", required=True, help="Logical database name (e.g. bronze)")
    parser.add_argument("--table", required=True, help="Table name (e.g. customer)")

    args = parser.parse_args()
    create_iceberg_table(args.warehouse, args.database, args.table)

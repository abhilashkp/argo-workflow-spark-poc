#!/usr/bin/env python3
"""
Iceberg Table Creation Script for Multi-Tenant Data Platform
Supports Polaris REST catalog and Azure Data Lake Storage with SAS tokens
"""

import argparse
import json
import os
import sys
from typing import List, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, TimestampType


def parse_arguments():
    """Parse command line arguments from the Go service"""
    parser = argparse.ArgumentParser(description="Create Iceberg table with multi-tenant support")
    
    # Basic table info
    parser.add_argument("--operation", required=True, help="Operation type (create_table)")
    parser.add_argument("--data-product", required=True, help="Data product name")
    parser.add_argument("--catalog-name", required=True, help="Catalog name")
    parser.add_argument("--catalog-type", required=True, help="Catalog type (rest, hive, etc.)")
    parser.add_argument("--catalog-warehouse", required=True, help="Catalog warehouse location")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--table-location", required=True, help="Table location in ADLS")
    
    # Schema fields (can have multiple)
    parser.add_argument("--schema-field", action="append", help="Schema field (name:type:required) - can be used multiple times")
    
    # Partition spec (can have multiple)
    parser.add_argument("--partition", action="append", help="Partition (name:transform) - can be used multiple times")
    
    # Properties (can have multiple)
    parser.add_argument("--property", action="append", help="Table property (key=value)")
    
    # Retention policy
    parser.add_argument("--retention-days", type=int, default=30, help="Snapshot retention days")
    
    # Catalog-specific (for REST/Polaris)
    parser.add_argument("--catalog-uri", help="REST catalog URI (for Polaris)")
    parser.add_argument("--catalog-credential", help="REST catalog credential")
    parser.add_argument("--catalog-token", help="REST catalog token")
    
    # Storage authentication
    parser.add_argument("--sas-token", required=True, help="SAS token for Azure storage access")
    
    return parser.parse_args()


def get_storage_config():
    """Get storage configuration from environment variables set by Go service"""
    return {
        "tenant_id": os.getenv("TENANT_ID"),
        "storage_account": os.getenv("StORAGE_ACCOUNT"),  # Note: matches Go service typo
        "container": os.getenv("CONTAINER"),
    }


def parse_schema_fields(args) -> List[Dict[str, Any]]:
    """Parse schema fields from command line arguments"""
    schema_fields = []
    
    if not args.schema_field:
        return schema_fields
    
    for field_arg in args.schema_field:
        # Parse format: name:type:required
        parts = field_arg.split(':')
        if len(parts) != 3:
            print(f"⚠️  Invalid schema field format: {field_arg}. Expected name:type:required")
            continue
            
        name, field_type, required = parts
        schema_fields.append({
            "name": name,
            "type": field_type.lower(),
            "required": required.lower() == "true"
        })
    
    return schema_fields


def parse_partition_spec(args) -> List[Dict[str, str]]:
    """Parse partition specification from command line arguments"""
    partitions = []
    
    if not args.partition:
        return partitions
        
    for partition_arg in args.partition:
        # Parse format: name:transform
        parts = partition_arg.split(':')
        if len(parts) != 2:
            print(f"⚠️  Invalid partition format: {partition_arg}. Expected name:transform")
            continue
            
        name, transform = parts
        partitions.append({
            "name": name,
            "transform": transform
        })
    
    return partitions


def parse_table_properties(args) -> Dict[str, str]:
    """Parse table properties from command line arguments"""
    properties = {}
    if args.property:
        for prop in args.property:
            if '=' in prop:
                key, value = prop.split('=', 1)
                properties[key] = value
            else:
                print(f"⚠️  Invalid property format: {prop}. Expected key=value")
    
    return properties


def map_type_to_spark(field_type: str) -> str:
    """Map field types to Spark SQL types"""
    type_mapping = {
        "string": "STRING",
        "int": "INT",
        "integer": "INT", 
        "long": "BIGINT",
        "bigint": "BIGINT",
        "double": "DOUBLE",
        "float": "FLOAT",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "decimal": "DECIMAL(10,2)"  # Default precision
    }
    
    return type_mapping.get(field_type.lower(), "STRING")


def build_schema_ddl(schema_fields: List[Dict[str, Any]]) -> str:
    """Build the DDL schema string for CREATE TABLE"""
    if not schema_fields:
        # Default schema if none provided
        return "id BIGINT, data STRING, created_at TIMESTAMP"
    
    schema_parts = []
    for field in schema_fields:
        spark_type = map_type_to_spark(field["type"])
        nullable = "" if field["required"] else ""  # Iceberg handles nullability differently
        schema_parts.append(f'{field["name"]} {spark_type}')
    
    return ",\n  ".join(schema_parts)


def build_partition_clause(partitions: List[Dict[str, str]]) -> str:
    """Build the partition clause for CREATE TABLE"""
    if not partitions:
        return ""
    
    partition_parts = []
    for partition in partitions:
        transform = partition["transform"]
        name = partition["name"]
        
        # Handle different partition transforms
        if transform == "identity":
            partition_parts.append(name)
        elif transform.startswith("bucket"):
            # bucket[N] format
            partition_parts.append(f"bucket({name}, {transform.split('[')[1].split(']')[0]})")
        elif transform.startswith("truncate"):
            # truncate[N] format  
            partition_parts.append(f"truncate({name}, {transform.split('[')[1].split(']')[0]})")
        elif transform == "year":
            partition_parts.append(f"years({name})")
        elif transform == "month":
            partition_parts.append(f"months({name})")
        elif transform == "day":
            partition_parts.append(f"days({name})")
        elif transform == "hour":
            partition_parts.append(f"hours({name})")
        else:
            # Default to identity
            partition_parts.append(name)
    
    return f"PARTITIONED BY ({', '.join(partition_parts)})"


def create_spark_session(args, storage_config: Dict[str, str]) -> SparkSession:
    """Create Spark session with proper Iceberg and storage configuration"""
    
    catalog_name = args.catalog_name
    builder = (
        SparkSession.builder
        .appName(f"CreateIcebergTable-{args.data_product}-{args.table}")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", args.catalog_type)
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"abfss://{storage_config['container']}@{storage_config['storage_account']}.dfs.core.windows.net/{args.data_product}")
    )
    
    # Add REST catalog specific configuration (for Polaris)
    if args.catalog_type == "rest" and args.catalog_uri:
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.uri", args.catalog_uri)
        
        if args.catalog_credential:
            builder = builder.config(f"spark.sql.catalog.{catalog_name}.credential", args.catalog_credential)
        
        if args.catalog_token:
            builder = builder.config(f"spark.sql.catalog.{catalog_name}.token", args.catalog_token)
    
    # Add Azure Storage configuration with SAS token
    if storage_config.get("storage_account") and args.sas_token:
        storage_account = storage_config["storage_account"]
        # Configure Azure storage with SAS token
        builder = builder.config(f"spark.hadoop.fs.azure.sas.{storage_config['container']}.{storage_account}.dfs.core.windows.net", 
                                args.sas_token)
    
    return builder.getOrCreate()


def create_iceberg_table(args):
    """Main function to create the Iceberg table"""
    print(f"🚀 Starting Iceberg table creation: {args.catalog_name}.{args.database}.{args.table}")
    
    # Get storage configuration
    storage_config = get_storage_config()
    print(f"📦 Storage Config - Account: {storage_config.get('storage_account')}, Container: {storage_config.get('container')}")
    print(f"🔐 SAS Token: {args.sas_token[:20]}..." if args.sas_token else "❌ No SAS token provided")
    
    # Parse schema and partitions
    schema_fields = parse_schema_fields(args)
    partitions = parse_partition_spec(args)
    properties = parse_table_properties(args)
    
    print(f"📋 Schema Fields: {len(schema_fields)} fields")
    print(f"🗂️  Partitions: {len(partitions)} partitions")
    print(f"⚙️  Properties: {len(properties)} properties")
    
    # Create Spark session
    spark = create_spark_session(args, storage_config)
    
    try:
        # Build CREATE TABLE statement
        full_table_name = f"{args.catalog_name}.{args.database}.{args.table}"
        schema_ddl = build_schema_ddl(schema_fields)
        partition_clause = build_partition_clause(partitions)
        
        # Build properties clause
        properties_list = []
        if properties:
            for key, value in properties.items():
                properties_list.append(f"'{key}' = '{value}'")
        
        # Add retention policy
        if args.retention_days:
            properties_list.append(f"'history.expire.max-snapshot-age-ms' = '{args.retention_days * 24 * 60 * 60 * 1000}'")
        
        properties_clause = ""
        if properties_list:
            properties_clause = f"TBLPROPERTIES ({', '.join(properties_list)})"
        
        # Construct full CREATE TABLE statement
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
          {schema_ddl}
        )
        USING iceberg
        LOCATION '{args.table_location}'
        {partition_clause}
        {properties_clause}
        """.strip()
        
        print(f"📝 Executing SQL:\n{create_sql}")
        
        # Execute the CREATE TABLE statement
        spark.sql(create_sql)
        
        print(f"✅ Successfully created Iceberg table: {full_table_name}")
        print(f"📍 Location: {args.table_location}")
        
        # Verify table creation
        spark.sql(f"DESCRIBE EXTENDED {full_table_name}").show(truncate=False)
        
    except Exception as e:
        print(f"❌ Error creating table: {str(e)}")
        raise
    finally:
        spark.stop()


def main():
    """Main entry point"""
    try:
        args = parse_arguments()
        
        if args.operation != "create_table":
            print(f"❌ Unsupported operation: {args.operation}")
            sys.exit(1)
        
        create_iceberg_table(args)
        print("🎉 Table creation completed successfully!")
        
    except Exception as e:
        print(f"💥 Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 
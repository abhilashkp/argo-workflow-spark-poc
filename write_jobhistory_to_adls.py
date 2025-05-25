import json
from datetime import datetime, timedelta
import argparse
import os
import urllib.parse
from pyspark.sql import SparkSession

def set_sas_token(spark, sas_token, account, container):
    decoded_token = urllib.parse.unquote(sas_token)
    conf_key = f"fs.azure.sas.{container}.{account}.dfs.core.windows.net"
    spark._jsc.hadoopConfiguration().set(conf_key, decoded_token)
    print(f"✅ Set Spark Hadoop config {conf_key}")

def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Write job run log to ADLS via Spark")
    parser.add_argument("--sastoken", required=True, help="SAS token for ADLS access")
    args = parser.parse_args()
    sas_token = args.sastoken
    print("🔐 Full SAS Token:", sas_token)

    # ADLS configuration
    storage_account_name = "bmdatalaketest"
    container_name = "table-maint-job-results"
    directory_path = "job-runs"

    # Initialize Spark session
    spark = SparkSession.builder.appName("JobHistoryWriter").getOrCreate()

    # Set SAS token in Hadoop config
    set_sas_token(spark, sas_token, storage_account_name, container_name)

    # Generate job metadata
    now = datetime.utcnow()
    start_time = now
    end_time = now + timedelta(minutes=6, seconds=30)

    job_run = {
        "tenantId": "tenant-001",
        "appName": "spark-table-maintenance",
        "startTime": start_time.isoformat(timespec='seconds') + "Z",
        "endTime": end_time.isoformat(timespec='seconds') + "Z",
        "status": "partial_success",
        "details": "Snapshot expiration succeeded, but compaction failed.",
        "operations": [
            {
                "type": "SnapshotExpiration",
                "status": "success",
                "details": "Expired snapshots for 1 table.",
                "startTime": start_time.isoformat(timespec='seconds') + "Z",
                "endTime": (start_time + timedelta(minutes=1, seconds=20)).isoformat(timespec='seconds') + "Z",
                "data": {"dataproduct": "marketing", "tables": ["campaigns"]}
            },
            {
                "type": "Compaction",
                "status": "failure",
                "details": "Compaction failed due to permission issue.",
                "startTime": (start_time + timedelta(minutes=1, seconds=30)).isoformat(timespec='seconds') + "Z",
                "endTime": (start_time + timedelta(minutes=3, seconds=45)).isoformat(timespec='seconds') + "Z",
                "data": {"dataproduct": "marketing", "numberOfFiles": 30}
            },
            {
                "type": "OrphanFiles",
                "status": "success",
                "details": "Identified and deleted orphan files.",
                "startTime": (start_time + timedelta(minutes=4)).isoformat(timespec='seconds') + "Z",
                "endTime": end_time.isoformat(timespec='seconds') + "Z",
                "data": {"dataproduct": "marketing", "numberOfFiles": 10}
            }
        ]
    }

    # Compose the file path
    app_name = os.getenv("APP_NAME", "default-spark-app")
    file_name = f"{app_name}.json"
    file_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{directory_path}/{file_name}"

    # Create RDD with JSON content
    file_contents = json.dumps(job_run, indent=2)
    rdd = spark.sparkContext.parallelize([file_contents])

    # Overwrite existing file (optional)
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(file_path)
    if fs.exists(path):
        fs.delete(path, True)

    # Write JSON as text file (single part file)
    rdd.coalesce(1).saveAsTextFile(file_path)

    print(f"✅ Job run log written to: {file_path}")

if __name__ == "__main__":
    main()

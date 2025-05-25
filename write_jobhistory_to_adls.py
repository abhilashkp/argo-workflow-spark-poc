import json
from datetime import datetime, timedelta
import argparse
import os
import urllib.parse
from pyspark.sql import SparkSession
# from py4j.java_gateway import java_import

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

    # Compose paths
    app_name = os.getenv("APP_NAME", "default-spark-app")
    temp_dir = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{directory_path}/temp_{app_name}"
    final_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{directory_path}/{app_name}.json"

    # Write job_run JSON as text
    json_str = json.dumps(job_run)
    df = spark.createDataFrame([json_str], "string")

    # Delete if exists (both temp and final)
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    spark_path = spark._jvm.org.apache.hadoop.fs.Path

    # if fs.exists(spark_path(final_path)):
    #     fs.delete(spark_path(final_path), True)
    # if fs.exists(spark_path(temp_dir)):
    #     fs.delete(spark_path(temp_dir), True)

    # Write to temp directory as a single part file
    df.coalesce(1).write.text(temp_dir)
    print(f"✅ Wrote intermediate file to: {temp_dir}")

    # Move part file to final destination as app_name.json
    file_status = fs.listStatus(spark_path(temp_dir))
    for status in file_status:
        name = status.getPath().getName()
        if name.startswith("part-") and name.endswith(".txt"):
            part_file_path = status.getPath()
            fs.rename(part_file_path, spark_path(final_path))
            print(f"✅ Renamed {part_file_path} to {final_path}")
            break

    # Cleanup temp directory
    fs.delete(spark_path(temp_dir), True)

    print(f"🎉 Job run log written to: {final_path}")

if __name__ == "__main__":
    main()

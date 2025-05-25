import json
from datetime import datetime, timedelta
import argparse
import os
import urllib.parse
from pyspark.sql import SparkSession
# from py4j.java_gateway import java_import

def set_sas_token(spark, sas_token, storage_account, container):
    decoded_token = urllib.parse.unquote(sas_token)
    formatted_token = f"?{sas_token}"
    print(f"🔑 Decoded SAS token: {decoded_token}")

    # conf_key = f"fs.azure.sas.{container}.{account}.dfs.core.windows.net"
    # spark._jsc.hadoopConfiguration().set(conf_key, decoded_token)
    print(f"🔐 Setting SAS token for {container} in {storage_account}...")
    

    # spark.conf.set("fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
    # spark.conf.set("fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    # spark.conf.set("fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", decoded_token)
    # spark.conf.set("fs.azure.sas.{container}.{storage_account}.dfs.core.windows.net",decoded_token)

    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.{container}.{storage_account}.dfs.core.windows.net", formatted_token)


    # resolved_cid: str = '695ae555-406e-41f4-93c1-5b85d68c5009'
    # resolved_cpwd: str = 'abAF2y_UEl2_aT5lj332~Cz.9_etM9HF8.'
    # resolved_tid: str = 'afadec18-0533-4cba-8578-5316252ff93f'
    # spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    # spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    #            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    # spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", resolved_cid)
    # spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", resolved_cpwd)
    # spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    #            f"https://login.microsoftonline.com/{resolved_tid}/oauth2/token")
    # print(f"✅ Set Spark Hadoop config {conf_key}")

def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Write job run log to ADLS via Spark")
    parser.add_argument("--sastoken", required=True, help="SAS token for ADLS access")
    args = parser.parse_args()
    sas_token = args.sastoken
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

    app_name = os.getenv("APP_NAME", "default-spark-app")
    print(f"📦 App Name: {app_name}")
    final_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{directory_path}/{app_name}.json"
    try:
    # Create DataFrame directly from dictionary (wrap in list to make a single-row DF)
        df = spark.createDataFrame([job_run])
    # Write JSON directly to final_path, overwrite if exists
        df.coalesce(1).write.mode("overwrite").json(final_path)
    except Exception as e:
        import traceback
        print("❌ Error while writing to ADLS:")
        traceback.print_exc()

    print(f"🎉 Job run log written to: {final_path}")

if __name__ == "__main__":
    main()
import json
from datetime import datetime, timedelta
from azure.storage.filedatalake import DataLakeServiceClient
import argparse
import os

def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Write job run log to ADLS")
    parser.add_argument("--sastoken", required=True, help="SAS token for ADLS access")
    args = parser.parse_args()
    sas_token = args.sas_token

    # Config values
    storage_account_name = "bmdatalaketest"
    container_name = "table-maint-job-results"
    directory_path = "job-runs"

    # Create service client
    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=sas_token
    )
    file_system_client = service_client.get_file_system_client(file_system=container_name)

    # Generate job metadata timestamps
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

    # Create directory if not exists
    directory_client = file_system_client.get_directory_client(directory_path)
    try:
        directory_client.create_directory()
    except Exception as e:
        # Directory might already exist — ignore or log
        pass

    app_name = os.getenv("APP_NAME", "default-spark-app")
    file_name = f"{app_name}.json"

    # Create file client and write JSON data
    file_client = directory_client.get_file_client(file_name)
    file_contents = json.dumps(job_run, indent=2)
    file_client.create_file()
    file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
    file_client.flush_data(len(file_contents))

    print(f"✅ Job run log written to: {directory_path}/{file_name}")

if __name__ == "__main__":
    main()

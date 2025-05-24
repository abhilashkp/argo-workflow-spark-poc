import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument('--sastoken', required=True, help="SAS token string")
parser.add_argument('--metadataconfig', required=True, help="JSON array of metadata")

args = parser.parse_args()

# Parse the JSON metadata array
try:
    metadata_list = json.loads(args.metadataconfig)
except json.JSONDecodeError:
    print("Error: Invalid JSON provided for --metadata")
    exit(1)

# Print the SAS token
print(f"SAS Token: {args.sastoken}")

# Print the metadata
print("Metadata:")
# Print the metadata JSON as a pretty-printed string
print("Metadata JSON:")
print(json.dumps(metadata_list, indent=2))

import argparse
import json
import os
from opensearchpy import OpenSearch, helpers
from dateutil.parser import parse as dateparse
from datetime import datetime, timezone, timedelta
import sys
from dotenv import load_dotenv
import os

# Load configurations .env file
load_dotenv()

if not os.environ.get("OPENSEARCH_HOST"):
   print("Please provide OPENSEARCH_HOST in the environment")
   sys.exit(1)


# Initialize OPENSEARCH client
def create_client():
    return OpenSearch(
        hosts=[{"host": os.environ.get("OPENSEARCH_HOST"), "port": os.environ.get("OPENSEARCH_PORT")}],
        http_auth=(os.environ.get("OPENSEARCH_USERNAME"), os.environ.get("OPENSEARCH_PASSWORD")),
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False
    )

# Load JSON file
def load_json_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Update timestamp of the documents
def update_timestamp(doc, shift):
    shifted_time = dateparse(doc['@timestamp']) + shift
    doc['@timestamp'] = shifted_time.isoformat()
    # print(doc['@timestamp'])
    return doc

# Get latest timestamp from the data
def latest_time(filename):
    with open(filename) as f:
        latest = None
        for line in f:
            doc = json.loads(line)
            if latest is None or dateparse(doc['@timestamp']) > dateparse(latest):
                latest = doc['@timestamp']
        # print(latest)
        return latest

# Get the required time shift to make documents current
def get_shift(latest):
    print(datetime.fromisoformat(latest))
    current_time = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5), 'America/New_York'))
    print(current_time)
    shift = current_time - datetime.fromisoformat(latest)
    print(shift)
    return shift

# Load data into OPENSEARCH
def make_actions(file_path):
    with open(file_path, 'r') as file:
        if file_path.endswith('.ndjson'):
            for line in file:
                yield json.loads(line)
        elif file_path.endswith('.csv'):
            import csv
            reader = csv.DictReader(file)
            for row in reader:
                yield row
        else:
            raise ValueError("Unsupported file format. Use .ndjson or .csv")

def load_data(client, index_name, actions):
    try:
        for success, item in helpers.bulk(client, actions, index=index_name):
            if not success:
                print(f"Error bulk-indexing a document")
    except Exception as e:
        print(f"Error bulk-indexing: {e}")
        return

# Main function
def main():
    parser = argparse.ArgumentParser(description="OPENSEARCH Data Loader")
    parser.add_argument('file', type=str, help="Path to the ndjson or csv file")
    parser.add_argument('--index', type=str, required=True, help="Target index name")
    parser.add_argument('--ingest-pipeline', type=str, help="Path to the ingest pipeline JSON file")
    parser.add_argument('--index-template', type=str, help="Path to the index template JSON file")
    parser.add_argument('--update-time', action='store_true', help="Update the @timestamp field values to current time")

    args = parser.parse_args()

    # Load configuration
    client = create_client()

    # Apply index template if provided
    if args.index_template:
        index_template = load_json_file(args.index_template)
        client.indices.put_index_template(name=args.index, body=index_template)

    # Apply ingest pipeline if provided
    if args.ingest_pipeline:
        ingest_pipeline = load_json_file(args.ingest_pipeline)
        client.ingest.put_pipeline(id=args.index, body=ingest_pipeline)

    # Update timestamp if required
    if args.update_time:
        shift = get_shift(latest_time(args.file))
        actions = (update_timestamp(doc, shift) for doc in make_actions(args.file))
    else:
        actions = make_actions(args.file)

    # Load data into OPENSEARCH
    load_data(client, args.index, actions)

if __name__ == "__main__":
    main()
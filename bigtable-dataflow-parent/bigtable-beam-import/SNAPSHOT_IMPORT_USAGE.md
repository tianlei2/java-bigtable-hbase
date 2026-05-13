# HBase Snapshot Import Helper Script Usage

This document describes the environment variables used by the `run-snapshot-import.sh` script to automate HBase snapshot imports into Cloud Bigtable using Dataflow.

## Environment Variables

The script relies on the following environment variables. You should set them before executing the script.

| Variable | Description | Example / Suggested Value |
| :--- | :--- | :--- |
| `PROJECT_ID` | The Google Cloud Project ID where the Bigtable instance and Dataflow jobs reside. | `your-project-id` |
| `INSTANCE_ID` | The Bigtable Instance ID to import data into. | `your-instance-id` |
| `BUCKET` | The GCS bucket name used for Dataflow staging, temp files, and default snapshot source path. | `your-gcs-bucket` |
| `REGION` | The GCP region to run the Dataflow jobs in. | `us-central1` |
| `TABLE_NAME` | The target Bigtable table name. | `your-table-name` |
| `SNAPSHOT_NAME` | The name of the HBase snapshot to import. | `your-snapshot-name` |
| `SNAPSHOT_SOURCE_DIR` | The GCS path where the HBase snapshot export is located. | `gs://your-gcs-bucket/snapshots` |
| `SERVICE_ACCOUNT` | The service account email to run the Dataflow jobs. | `your-service-account@developer.gserviceaccount.com` |
| `NUM_SHARDS` | The number of shards to split the import into for parallel processing. | `20` |
| `MAX_INFLIGHT_RPCS` | Maximum number of inflight RPCs for Bigtable client. | `100` |
| `BULK_MUTATION_CLOSE_TIMEOUT_MINUTES` | Timeout in minutes for closing bulk mutations. | `30` |
| `NETWORK` | VPC Network name for Dataflow workers. | `your-network` |
| `SUBNETWORK` | VPC Subnetwork name for Dataflow workers. | `regions/us-central1/subnetworks/your-subnetwork` |

## Usage

### Run a specific shard range
```bash
./run-snapshot-import.sh <start_shard> <end_shard>
```
Example: `./run-snapshot-import.sh 0 5`

### Run all shards (Auto-parallel mode)
```bash
./run-snapshot-import.sh --all
```
This mode will first run the restore step, and then launch background processes for all shards in parallel.

## Advanced Usage

### Manual Parallel Execution

To run shards in parallel groups (e.g., assuming 20 shards total), you can run multiple instances of this script.

> [!IMPORTANT]
> Shard 0 performs the restore step. You MUST run the first group (including shard 0) first and let it complete the restore step before launching other groups in parallel. Otherwise, they will fail because the restored files won't exist yet!

Example for manual parallel execution:
```bash
./run-snapshot-import.sh 0 3 &  # Run this first!
# Wait for shard 0 to finish restore, then run the rest:
./run-snapshot-import.sh 4 7 &
./run-snapshot-import.sh 8 11 &
./run-snapshot-import.sh 12 15 &
./run-snapshot-import.sh 16 19 &
```

## Troubleshooting

### JDK Compatibility

If you are running on a newer JDK (like Java 21 or 26) and hit ByteBuddy errors, you can add `-Dnet.bytebuddy.experimental=true` to the `java` command lines in the script.

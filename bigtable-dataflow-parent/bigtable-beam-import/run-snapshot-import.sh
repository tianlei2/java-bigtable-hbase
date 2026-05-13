#!/bin/bash

# ==============================================================================
# HBase Snapshot Import Helper Script
# ==============================================================================
# This script runs a range of Dataflow snapshot import jobs sequentially or in parallel.
# Must be executed from the 'bigtable-dataflow-parent/bigtable-beam-import' directory.
#
# For detailed usage and advanced options, see: SNAPSHOT_IMPORT_USAGE.md
# ==============================================================================

# ------------------------------------------------------------------------------
# Environment Variables
# ------------------------------------------------------------------------------
# Most users will need to set these variables before running the script.
# See SNAPSHOT_IMPORT_USAGE.md for details and expected values.

# --- Required / Common Configurations ---
# export PROJECT_ID="your-project-id"
# export INSTANCE_ID="your-instance-id"
# export BUCKET="your-gcs-bucket"
# export REGION="us-central1"
# 
# export TABLE_NAME="your-table-name"
# export SNAPSHOT_NAME="your-snapshot-name"
# export SNAPSHOT_SOURCE_DIR="gs://your-gcs-bucket/snapshots"
# export SERVICE_ACCOUNT="your-service-account"

# --- Sharding & Tuning ---
# export NUM_SHARDS="20"
# export MAX_INFLIGHT_RPCS="100"
# export BULK_MUTATION_CLOSE_TIMEOUT_MINUTES="30"

# --- Network Configurations ---
# export NETWORK="your-network"
# export SUBNETWORK="your-subnetwork"

# ------------------------------------------------------------------------------
# Usage
# ------------------------------------------------------------------------------
# Usage: ./run-snapshot-import.sh <start_shard> <end_shard>
#   Or:  ./run-snapshot-import.sh --all
#        (Runs all shards in parallel groups of 4 by default)
#
# Examples:
#   ./run-snapshot-import.sh 0 3
#   ./run-snapshot-import.sh --all

if [ "$#" -ne 2 ] && [ "$1" != "--all" ]; then
    echo "Usage: $0 <start_shard> <end_shard>"
    echo "   Or: $0 --all"
    exit 1
fi

START_SHARD=$1
END_SHARD=$2

# Configurations
JAR_PATH="target/bigtable-beam-import-2.17.0-shaded.jar"

# --- AUTO-PARALLEL MODE ---
if [ "$1" == "--all" ]; then
    echo "🚀 Starting fully automated snapshot import..."
    
    # Step 1: Perform ONLY the restore step
    echo "Step 1/2: Performing snapshot restore (blocking)..."
    java -jar ${JAR_PATH} importsnapshot \
      --runner=DataflowRunner \
      --project=${PROJECT_ID} \
      --bigtableInstanceId=${INSTANCE_ID} \
      --bigtableTableId=${TABLE_NAME} \
      --hbaseSnapshotSourceDir=${SNAPSHOT_SOURCE_DIR} \
      --snapshots=${SNAPSHOT_NAME}:${TABLE_NAME} \
      --stagingLocation=gs://${BUCKET}/dataflow/staging \
      --tempLocation=gs://${BUCKET}/dataflow/temp \
      --region=${REGION} \
      --performOnlyRestoreStep=true \
      --jobName="restore-job" \
      --network=${NETWORK} \
      --subnetwork=${SUBNETWORK}
      
    echo "Restore completed. Proceeding to data import."

    # Step 2: Launch parallel groups of 4
    echo "Step 2/2: Launching parallel groups of 4 shards..."
    SHARDS_PER_GROUP=4
    
    for (( start=0; start<$NUM_SHARDS; start+=$SHARDS_PER_GROUP )); do
        end=$((start + SHARDS_PER_GROUP - 1))
        [ $end -ge $NUM_SHARDS ] && end=$((NUM_SHARDS - 1))
        
        echo "Launching group: shards $start to $end in background"
        # Call ourselves with the range!
        $0 $start $end &
    done
    
    echo "All groups launched. Waiting for all background jobs to finish..."
    wait
    echo "🎉 All import jobs completed!"
    exit 0
fi
# ----------------------------------------

# Standard Range Mode
for i in $(seq $START_SHARD $END_SHARD); do
  echo "Submitting Dataflow job for shardIndex: $i"

  # We skip restore for all shards if running via --all because Step 1 handled it.
  # If running manually via ranges, shard 0 will perform restore.
  SKIP_RESTORE="true"
  if [ $i -eq 0 ]; then
    SKIP_RESTORE="false"
  fi
  
  JOB="job-${i}"
  java -jar ${JAR_PATH} importsnapshot \
    --runner=DataflowRunner \
    --project=${PROJECT_ID} \
    --bigtableInstanceId=${INSTANCE_ID} \
    --bigtableTableId=${TABLE_NAME} \
    --hbaseSnapshotSourceDir=${SNAPSHOT_SOURCE_DIR} \
    --snapshots=${SNAPSHOT_NAME}:${TABLE_NAME} \
    --stagingLocation=gs://${BUCKET}/dataflow/staging \
    --tempLocation=gs://${BUCKET}/dataflow/temp \
    --workerMachineType=n1-highmem-4 \
    --diskSizeGb=500 \
    --maxNumWorkers=10 \
    --region=${REGION} \
    --serviceAccount=${SERVICE_ACCOUNT} \
    --usePublicIps=false \
    --enableSnappy=true \
    --skipRestoreStep=${SKIP_RESTORE} \
    --numShards=${NUM_SHARDS} \
    --shardIndex=$i \
    --jobName="${JOB}" \
    --network=${NETWORK} \
    --subnetwork=${SUBNETWORK} \
    --maxInflightRpcs=${MAX_INFLIGHT_RPCS} \
    --bulkMutationCloseTimeoutMinutes=${BULK_MUTATION_CLOSE_TIMEOUT_MINUTES}

  # Sequential within this script instance
done

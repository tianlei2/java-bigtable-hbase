/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.beam.hbasesnapshots.dofn;

import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link DoFn} class for converting Hbase {@link Result} to list of Hbase {@link Mutation}s. */
public class CreateBigtableMutations
    extends DoFn<KV<SnapshotConfig, Result>, KV<String, Iterable<Mutation>>> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateBigtableMutations.class);

  private static final Counter droppedRows =
      Metrics.counter(CreateBigtableMutations.class, "droppedRows");
  private static final Counter droppedCells =
      Metrics.counter(CreateBigtableMutations.class, "droppedCells");
  private static final Counter droppedRowKeys =
      Metrics.counter(CreateBigtableMutations.class, "droppedRowKeys");
  private final int maxMutationsPerRequestThreshold;

  private final boolean filterLargeRows;
  private final long filterLargeRowThresholdBytes;

  private final boolean filterLargeCells;
  private final int filterLargeCellThresholdBytes;

  private final boolean filterLargeRowKeys;
  private final int filterLargeRowKeysThresholdBytes;

  public CreateBigtableMutations(
      int maxMutationsPerRequestThreshold,
      boolean filterLargeRows,
      long filterLargeRowThresholdBytes,
      boolean filterLargeCells,
      int filterLargeCellThresholdBytes,
      boolean filterLargeRowKeys,
      int filterLargeRowKeysThresholdBytes) {

    this.maxMutationsPerRequestThreshold = maxMutationsPerRequestThreshold;

    this.filterLargeRows = filterLargeRows;
    this.filterLargeRowThresholdBytes = filterLargeRowThresholdBytes;

    this.filterLargeCells = filterLargeCells;
    this.filterLargeCellThresholdBytes = filterLargeCellThresholdBytes;

    this.filterLargeRowKeys = filterLargeRowKeys;
    this.filterLargeRowKeysThresholdBytes = filterLargeRowKeysThresholdBytes;
  }

  @ProcessElement
  public void processElement(
      @Element KV<SnapshotConfig, Result> element,
      OutputReceiver<KV<String, Iterable<Mutation>>> outputReceiver)
      throws IOException {
    if (element.getValue().isEmpty()) {
      return;
    }
    String targetTable = element.getKey().getTableName();
    String snapshotName = element.getKey().getSnapshotName();

    // Limit the number of mutations per Put (server will reject >= 100k mutations per Put)
    byte[] rowKey = element.getValue().getRow();

    // Hoist row key size check to the top to fail-fast and avoid expensive allocations.
    // We also use Beam metrics to surface data loss instantly.
    if (filterLargeRowKeys && rowKey.length > filterLargeRowKeysThresholdBytes) {
      LOG.warn(
          "For snapshot "
              + snapshotName
              + ": Dropping row, row key length, "
              + rowKey.length
              + ", exceeds filter length threshold, "
              + filterLargeRowKeysThresholdBytes
              + ", row key: "
              + Bytes.toStringBinary(rowKey));
      droppedRowKeys.inc();
      return;
    }

    List<Mutation> mutations = new ArrayList<>();

    boolean logAndSkipIncompatibleRowMutations =
        convertAndValidateThresholds(
            rowKey, element.getValue().listCells(), mutations, snapshotName);

    if (!logAndSkipIncompatibleRowMutations && !mutations.isEmpty()) {
      outputReceiver.output(KV.of(targetTable, mutations));
    }
  }

  private boolean convertAndValidateThresholds(
      byte[] rowKey, List<Cell> cells, List<Mutation> mutations, String snapshotName)
      throws IOException {
    boolean logAndSkipIncompatibleRows = false;

    Put put = null;
    int cellCount = 0;
    long totalByteSize = 0L;
    boolean loggedLargeCellForRow = false;

    // create mutations
    for (Cell cell : cells) {
      totalByteSize += cell.heapSize();

      // Check threshold inside loop to fail-fast and avoid OOM by allocating too many mutations.
      if (filterLargeRows && totalByteSize > filterLargeRowThresholdBytes) {
        logAndSkipIncompatibleRows = true;
        LOG.warn(
            "For snapshot "
                + snapshotName
                + ": Dropping row, row length, "
                + totalByteSize
                + ", exceeds filter length threshold, "
                + filterLargeRowThresholdBytes
                + ", row key: "
                + Bytes.toStringBinary(rowKey));
        droppedRows.inc();
        mutations.clear();
        break;
      }

      // handle large cells
      if (filterLargeCells && cell.getValueLength() > filterLargeCellThresholdBytes) {
        if (!loggedLargeCellForRow) {
          LOG.warn(
              "For snapshot "
                  + snapshotName
                  + ": Dropping large cells for row "
                  + Bytes.toStringBinary(rowKey)
                  + ". At least one cell exceeds threshold "
                  + filterLargeCellThresholdBytes);
          loggedLargeCellForRow = true;
        }
        droppedCells.inc();
        continue;
      }

      // Split the row into multiple mutations if mutations exceeds threshold limit
      if (cellCount % maxMutationsPerRequestThreshold == 0) {
        cellCount = 0;
        put = new Put(rowKey);
        mutations.add(put);
      }
      put.add(cell);
      cellCount++;
    }

    return logAndSkipIncompatibleRows;
  }
}

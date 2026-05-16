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

import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.transforms.HbaseRegionSplitTracker;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Splittable {@link DoFn} for reading the records from each region. */
public class ReadSnapshotRegion extends DoFn<RegionConfig, KV<SnapshotConfig, Result>> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadSnapshotRegion.class);

  private static final long BYTES_PER_SPLIT = 512 * 1024 * 1024; // 512 MB
  private static final long BYTES_PER_GB = 1024 * 1024 * 1024;

  private final boolean useDynamicSplitting;

  public ReadSnapshotRegion(boolean useDynamicSplitting) {
    this.useDynamicSplitting = useDynamicSplitting;
  }

  @ProcessElement
  public void processElement(
      @Element RegionConfig regionConfig,
      OutputReceiver<KV<SnapshotConfig, Result>> outputReceiver,
      RestrictionTracker<ByteKeyRange, ByteKey> tracker)
      throws Exception {

    try (HBaseRegionScanner scanner = newScanner(regionConfig, tracker.currentRestriction())) {
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        if (tracker.tryClaim(ByteKey.copyFrom(result.getRow()))) {
          outputReceiver.output(KV.of(regionConfig.getSnapshotConfig(), result));
        } else {
          return;
        }
      }
      // Scanner exhausted naturally. Claim the end key to mark done.
      tracker.tryClaim(tracker.currentRestriction().getEndKey());
    }
  }

  HBaseRegionScanner newScanner(RegionConfig regionConfig, ByteKeyRange byteKeyRange)
      throws Exception {
    Scan scan =
        new Scan()
            .withStartRow(byteKeyRange.getStartKey().getBytes())
            .withStopRow(byteKeyRange.getEndKey().getBytes())
            .setIsolationLevel(IsolationLevel.READ_UNCOMMITTED)
            .setCacheBlocks(false);

    SnapshotConfig snapshotConfig = regionConfig.getSnapshotConfig();

    Path sourcePath = snapshotConfig.getSourcePath();
    Path restorePath = snapshotConfig.getRestorePath();
    Configuration configuration = snapshotConfig.getConfiguration();
    FileSystem fileSystem = sourcePath.getFileSystem(configuration);

    return new HBaseRegionScanner(
        configuration,
        fileSystem,
        restorePath,
        regionConfig.getTableDescriptor(),
        regionConfig.getRegionInfo(),
        scan);
  }

  @GetInitialRestriction
  public ByteKeyRange getInitialRange(@Element RegionConfig regionConfig) {
    return ByteKeyRange.of(
        ByteKey.copyFrom(regionConfig.getRegionInfo().getStartKey()),
        ByteKey.copyFrom(regionConfig.getRegionInfo().getEndKey()));
  }

  @GetSize
  public double getSize(@Element RegionConfig regionConfig) {
    return BYTES_PER_SPLIT;
  }

  @NewTracker
  public HbaseRegionSplitTracker newTracker(
      @Element RegionConfig regionConfig, @Restriction ByteKeyRange range) {
    return new HbaseRegionSplitTracker(
        regionConfig.getSnapshotConfig().getSnapshotName(),
        regionConfig.getRegionInfo().getEncodedName(),
        range,
        useDynamicSplitting);
  }

  @SplitRestriction
  public void splitRestriction(
      @Element RegionConfig regionConfig,
      @Restriction ByteKeyRange range,
      OutputReceiver<ByteKeyRange> outputReceiver) {
    try {
      int numSplits = getSplits(regionConfig.getRegionSize());
      LOG.info(
          "Splitting Initial Restriction for SnapshotName: {} - regionname:{} - regionsize(GB):{}"
              + " - Splits: {}",
          regionConfig.getSnapshotConfig().getSnapshotName(),
          regionConfig.getRegionInfo().getEncodedName(),
          (double) regionConfig.getRegionSize() / BYTES_PER_GB,
          numSplits);
      if (numSplits > 1) {
        RegionSplitter.UniformSplit uniformSplit = new RegionSplitter.UniformSplit();
        byte[][] splits =
            uniformSplit.split(
                range.getStartKey().getBytes(),
                range.getEndKey().getBytes(),
                getSplits(regionConfig.getRegionSize()),
                true);
        for (int i = 0; i < splits.length - 1; i++) {
          outputReceiver.output(
              ByteKeyRange.of(ByteKey.copyFrom(splits[i]), ByteKey.copyFrom(splits[i + 1])));
        }
      } else {
        outputReceiver.output(range);
      }
    } catch (Exception ex) {
      LOG.warn(
          "Unable to split range for region:{} in Snapshot:{}",
          regionConfig.getRegionInfo().getEncodedName(),
          regionConfig.getSnapshotConfig().getSnapshotName());
      outputReceiver.output(range);
    }
  }

  private int getSplits(long sizeInBytes) {
    return (int) Math.ceil((double) sizeInBytes / BYTES_PER_SPLIT);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(
        DisplayData.item("DynamicSplitting", useDynamicSplitting ? "Enabled" : "Disabled")
            .withLabel("Dynamic Splitting"));
  }
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A workalike for {@link org.apache.hadoop.hbase.client.ClientSideRegionScanner}.
 *
 * <p>It serves the same purpose, but skips block and mobFile cache initialization. Those caches
 * dont appear to useful for the import job and leak threads on shutdown
 */
public class HBaseRegionScanner implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseRegionScanner.class);

  private HRegion region;
  private RegionScanner scanner;
  private final List<Cell> values;
  private boolean regionOperationStarted = false;
  private boolean hasMore = true;

  public HBaseRegionScanner(
      Configuration originalConf,
      FileSystem fs,
      Path rootDir,
      TableDescriptor htd,
      RegionInfo hri,
      Scan scan)
      throws IOException {
    Configuration conf = new Configuration(originalConf);
    scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
    htd = TableDescriptorBuilder.newBuilder(htd).setReadOnly(true).build();
    this.region =
        HRegion.newHRegion(
            CommonFSUtils.getTableDir(rootDir, htd.getTableName()),
            (WAL) null,
            fs,
            conf,
            hri,
            htd,
            (RegionServerServices) null);
    this.region.setRestoredRegion(true);
    conf.set("hfile.block.cache.policy", "IndexOnlyLRU");
    conf.setIfUnset("hfile.onheap.block.cache.fixed.size", String.valueOf(33554432L));
    conf.unset("hbase.bucketcache.ioengine");
    // Disable background threads (compactions and cache flushes) to prevent leaks on workers.
    conf.setInt("hbase.hstore.compactionThreshold", 10000);
    conf.setLong("hbase.regionserver.optionalcacheflushinterval", 0);
    conf.setInt("hbase.client.retries.number", 3);

    // Wrap in try-catch to ensure close() is called on failure, avoiding leaks.
    try {
      this.region.initialize();
      this.scanner = this.region.getScanner(scan);
      this.values = new ArrayList();

      this.region.startRegionOperation();
      this.regionOperationStarted = true;
    } catch (IOException e) {
      LOG.error("Failed to initialize HBaseRegionScanner", e);
      if (this.region != null) {
        try {
          this.region.close(true);
        } catch (IOException e2) {
          LOG.warn("Exception while closing region after initialization failure", e2);
        }
      }
      throw e;
    }
  }

  public void close() {
    if (this.scanner != null) {
      try {
        this.scanner.close();
        this.scanner = null;
      } catch (IOException var3) {
        LOG.warn("Exception while closing scanner", var3);
      }
    }

    if (this.region != null) {
      try {
        if (this.regionOperationStarted) {
          this.region.closeRegionOperation();
        }
        this.region.close(true);
        this.region = null;
      } catch (IOException var2) {
        LOG.warn("Exception while closing region", var2);
      }
    }
  }

  public Result next() throws IOException {
    do {
      if (!this.hasMore) {
        return null;
      }

      this.values.clear();
      this.hasMore = this.scanner.nextRaw(this.values);
    } while (this.values.isEmpty());

    Result result = Result.create(this.values);

    return result;
  }
}

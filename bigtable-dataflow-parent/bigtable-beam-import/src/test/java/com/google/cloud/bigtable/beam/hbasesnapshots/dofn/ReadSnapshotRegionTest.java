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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the {@link ReadSnapshotRegion} functionality. */
@RunWith(JUnit4.class)
public class ReadSnapshotRegionTest {

  private RegionConfig regionConfig;
  private SnapshotConfig snapshotConfig;
  private org.apache.hadoop.hbase.client.RegionInfo regionInfo;

  @Before
  public void setUp() {
    regionConfig = mock(RegionConfig.class);
    snapshotConfig = mock(SnapshotConfig.class);
    regionInfo = mock(org.apache.hadoop.hbase.client.RegionInfo.class);

    when(regionConfig.getSnapshotConfig()).thenReturn(snapshotConfig);
    when(regionConfig.getRegionInfo()).thenReturn(regionInfo);
    when(snapshotConfig.getSnapshotName()).thenReturn("test-snapshot");
    when(regionInfo.getEncodedName()).thenReturn("test-region");
  }

  /**
   * Tests that {@link ReadSnapshotRegion#processElement} successfully reads and outputs all records
   * from the region when no splitting occurs.
   */
  @Test
  public void testProcessElement() throws Exception {
    RestrictionTracker<ByteKeyRange, ByteKey> tracker = mock(RestrictionTracker.class);
    ByteKeyRange range =
        ByteKeyRange.of(ByteKey.copyFrom("a".getBytes()), ByteKey.copyFrom("z".getBytes()));
    when(tracker.currentRestriction()).thenReturn(range);

    HBaseRegionScanner scanner = mock(HBaseRegionScanner.class);
    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    when(result1.getRow()).thenReturn("b".getBytes());
    when(result2.getRow()).thenReturn("c".getBytes());

    when(scanner.next()).thenReturn(result1, result2, null);

    when(tracker.tryClaim(ByteKey.copyFrom("b".getBytes()))).thenReturn(true);
    when(tracker.tryClaim(ByteKey.copyFrom("c".getBytes()))).thenReturn(true);

    ReadSnapshotRegion fn = new TestReadSnapshotRegion(true, scanner);

    DoFn.OutputReceiver<KV<SnapshotConfig, Result>> receiver = mock(DoFn.OutputReceiver.class);

    fn.processElement(regionConfig, receiver, tracker);

    verify(receiver, times(1)).output(KV.of(snapshotConfig, result1));
    verify(receiver, times(1)).output(KV.of(snapshotConfig, result2));

    verify(tracker, times(1)).tryClaim(ByteKey.EMPTY);
  }

  /**
   * Tests that {@link ReadSnapshotRegion#processElement} stops processing when the restriction
   * tracker refuses a claim (e.g. when the restriction has been split).
   */
  @Test
  public void testProcessElement_StopOnClaimFailure() throws Exception {
    RestrictionTracker<ByteKeyRange, ByteKey> tracker = mock(RestrictionTracker.class);
    ByteKeyRange range =
        ByteKeyRange.of(ByteKey.copyFrom("a".getBytes()), ByteKey.copyFrom("z".getBytes()));
    when(tracker.currentRestriction()).thenReturn(range);

    HBaseRegionScanner scanner = mock(HBaseRegionScanner.class);
    Result result1 = mock(Result.class);
    Result result2 = mock(Result.class);
    when(result1.getRow()).thenReturn("b".getBytes());
    when(result2.getRow()).thenReturn("c".getBytes());

    when(scanner.next()).thenReturn(result1, result2, null);

    when(tracker.tryClaim(ByteKey.copyFrom("b".getBytes()))).thenReturn(true);
    when(tracker.tryClaim(ByteKey.copyFrom("c".getBytes()))).thenReturn(false);

    ReadSnapshotRegion fn = new TestReadSnapshotRegion(true, scanner);

    DoFn.OutputReceiver<KV<SnapshotConfig, Result>> receiver = mock(DoFn.OutputReceiver.class);

    fn.processElement(regionConfig, receiver, tracker);

    verify(receiver, times(1)).output(KV.of(snapshotConfig, result1));
    verify(receiver, times(0)).output(KV.of(snapshotConfig, result2));

    verify(tracker, times(1)).tryClaim(ByteKey.EMPTY);
  }

  /**
   * Tests that {@link ReadSnapshotRegion#splitRestriction} does not split the restriction when the
   * region size is smaller than the split threshold.
   */
  @Test
  public void testSplitRestriction_NoSplit() throws Exception {
    when(regionConfig.getRegionSize()).thenReturn(100L * 1024 * 1024); // 100 MB

    ByteKeyRange range =
        ByteKeyRange.of(ByteKey.copyFrom("a".getBytes()), ByteKey.copyFrom("z".getBytes()));

    DoFn.OutputReceiver<ByteKeyRange> receiver = mock(DoFn.OutputReceiver.class);

    ReadSnapshotRegion fn = new ReadSnapshotRegion(true);
    fn.splitRestriction(regionConfig, range, receiver);

    verify(receiver, times(1)).output(range);
  }

  /**
   * Tests that {@link ReadSnapshotRegion#splitRestriction} splits the restriction into multiple
   * sub-ranges when the region size exceeds the split threshold.
   */
  @Test
  public void testSplitRestriction_WithSplit() throws Exception {
    when(regionConfig.getRegionSize()).thenReturn(1500L * 1024 * 1024); // ~1.5 GB -> 3 splits

    ByteKeyRange range =
        ByteKeyRange.of(ByteKey.copyFrom("a".getBytes()), ByteKey.copyFrom("z".getBytes()));

    DoFn.OutputReceiver<ByteKeyRange> receiver = mock(DoFn.OutputReceiver.class);

    ReadSnapshotRegion fn = new ReadSnapshotRegion(true);
    fn.splitRestriction(regionConfig, range, receiver);

    verify(receiver, times(3)).output(org.mockito.ArgumentMatchers.any(ByteKeyRange.class));
  }

  /**
   * Tests that {@link ReadSnapshotRegion#splitRestriction} falls back to the original restriction
   * when an exception occurs during splitting.
   */
  @Test
  public void testSplitRestriction_Exception() throws Exception {
    when(regionConfig.getRegionSize()).thenThrow(new RuntimeException("test exception"));

    ByteKeyRange range =
        ByteKeyRange.of(ByteKey.copyFrom("a".getBytes()), ByteKey.copyFrom("z".getBytes()));

    DoFn.OutputReceiver<ByteKeyRange> receiver = mock(DoFn.OutputReceiver.class);

    ReadSnapshotRegion fn = new ReadSnapshotRegion(true);
    fn.splitRestriction(regionConfig, range, receiver);

    verify(receiver, times(1)).output(range);
  }

  static class TestReadSnapshotRegion extends ReadSnapshotRegion {
    private final HBaseRegionScanner mockScanner;

    public TestReadSnapshotRegion(boolean useDynamicSplitting, HBaseRegionScanner mockScanner) {
      super(useDynamicSplitting);
      this.mockScanner = mockScanner;
    }

    @Override
    HBaseRegionScanner newScanner(RegionConfig regionConfig, ByteKeyRange byteKeyRange) {
      return mockScanner;
    }
  }
}

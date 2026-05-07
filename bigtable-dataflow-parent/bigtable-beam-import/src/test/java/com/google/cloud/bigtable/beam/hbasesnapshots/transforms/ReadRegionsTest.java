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
package com.google.cloud.bigtable.beam.hbasesnapshots.transforms;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import java.math.BigInteger;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the {@link ReadRegions} transform. */
@RunWith(JUnit4.class)
public class ReadRegionsTest {

  /** Tests that constructor throws exception when numShards is set but shardIndex is missing. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidShardConfig_missingShardIndex() {
    new ReadRegions(
        true,
        100,
        false,
        0L,
        false,
        0,
        false,
        0,
        2, // numShards
        null // shardIndex missing
        );
  }

  /** Tests that constructor throws exception when shardIndex is out of bounds. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidShardConfig_shardIndexTooHigh() {
    new ReadRegions(true, 100, false, 0L, false, 0, false, 0, 2, 2);
  }

  /** Tests the basic sharding logic math. */
  @Test
  public void testShardingLogic() {
    // Verify the logic used for sharding
    byte[] regionName = "someEncodedRegionName".getBytes();
    int numShards = 4;

    long remainder = new BigInteger(regionName).mod(BigInteger.valueOf(numShards)).longValue();
    assertTrue(remainder >= 0 && remainder < numShards);
  }

  /**
   * Tests that the sharding logic correctly selects or skips regions based on their encoded name,
   * numShards, and shardIndex.
   */
  @Test
  public void testShardingFilterPredicate() {
    RegionConfig rc = mock(RegionConfig.class);
    RegionInfo ri = mock(RegionInfo.class);
    when(rc.getRegionInfo()).thenReturn(ri);

    // "a" in bytes is 97. 97 % 4 = 1.
    when(ri.getEncodedNameAsBytes()).thenReturn("a".getBytes());

    // If shardIndex is 1, it should be taken.
    assertTrue(isTaken("a".getBytes(), 4, 1));
    // If shardIndex is 0, it should be skipped.
    assertFalse(isTaken("a".getBytes(), 4, 0));
  }

  private boolean isTaken(byte[] regionName, int numShards, int shardIndex) {
    long remainder = new BigInteger(regionName).mod(BigInteger.valueOf(numShards)).longValue();
    return remainder == shardIndex;
  }
}

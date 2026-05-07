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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Tests the {@link CreateBigtableMutations} functionality. */
@RunWith(JUnit4.class)
public class CreateBigtableMutationsTest {

  private SnapshotConfig snapshotConfig;
  private Result result;
  private DoFn.OutputReceiver<KV<String, Iterable<Mutation>>> receiver;

  @Before
  public void setUp() {
    snapshotConfig = mock(SnapshotConfig.class);
    result = mock(Result.class);
    receiver = mock(DoFn.OutputReceiver.class);

    when(snapshotConfig.getTableName()).thenReturn("my-table");
    when(snapshotConfig.getSnapshotName()).thenReturn("my-snapshot");
  }

  /**
   * Tests that {@link CreateBigtableMutations#processElement} successfully converts a simple HBase
   * result to Bigtable mutations.
   */
  @Test
  public void testProcessElement() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    Cell cell = new KeyValue(rowKey, "cf".getBytes(), "qual".getBytes(), "val".getBytes());

    when(result.listCells()).thenReturn(Collections.singletonList(cell));
    when(result.getRow()).thenReturn(rowKey);

    CreateBigtableMutations fn = new CreateBigtableMutations(100, false, 0L, false, 0, false, 0);

    fn.processElement(KV.of(snapshotConfig, result), receiver);

    verify(receiver, times(1)).output(Mockito.any());
  }

  /** Tests that {@link CreateBigtableMutations#processElement} skips results with no cells. */
  @Test
  public void testProcessElement_emptyCells() throws Exception {
    when(result.listCells()).thenReturn(Collections.emptyList());

    CreateBigtableMutations fn = new CreateBigtableMutations(100, false, 0L, false, 0, false, 0);

    fn.processElement(KV.of(snapshotConfig, result), receiver);

    verify(receiver, times(0)).output(Mockito.any());
  }

  /**
   * Tests that {@link CreateBigtableMutations#processElement} filters out cells that exceed the
   * configured size threshold.
   */
  @Test
  public void testProcessElement_filterLargeCell() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    byte[] largeValue = new byte[1000];
    Cell cell = new KeyValue(rowKey, "cf".getBytes(), "qual".getBytes(), largeValue);

    when(result.listCells()).thenReturn(Collections.singletonList(cell));
    when(result.getRow()).thenReturn(rowKey);

    CreateBigtableMutations fn =
        new CreateBigtableMutations(
            100, false, 0L, true, 500, // filterLargeCells enabled, threshold 500
            false, 0);

    fn.processElement(KV.of(snapshotConfig, result), receiver);

    verify(receiver, times(0)).output(Mockito.any());
  }

  /**
   * Tests that {@link CreateBigtableMutations#processElement} drops the row if the total size of
   * its cells exceeds the configured threshold.
   */
  @Test
  public void testProcessElement_filterLargeRow() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    Cell cell = new KeyValue(rowKey, "cf".getBytes(), "qual".getBytes(), "val".getBytes());

    when(result.listCells()).thenReturn(Collections.singletonList(cell));
    when(result.getRow()).thenReturn(rowKey);

    CreateBigtableMutations fn =
        new CreateBigtableMutations(
            100, true, 10L, false, 0, false, 0 // filterLargeRows enabled, threshold 10 bytes
            );

    fn.processElement(KV.of(snapshotConfig, result), receiver);

    verify(receiver, times(0)).output(Mockito.any());
  }

  /**
   * Tests that {@link CreateBigtableMutations#processElement} drops the row if the row key length
   * exceeds the configured threshold.
   */
  @Test
  public void testProcessElement_filterLargeRowKey() throws Exception {
    byte[] rowKey = "row-key".getBytes(); // 7 bytes
    Cell cell = new KeyValue(rowKey, "cf".getBytes(), "qual".getBytes(), "val".getBytes());

    when(result.listCells()).thenReturn(Collections.singletonList(cell));
    when(result.getRow()).thenReturn(rowKey);

    CreateBigtableMutations fn =
        new CreateBigtableMutations(
            100, false, 0L, false, 0, true, 5 // filterLargeRowKeys enabled, threshold 5 bytes
            );

    fn.processElement(KV.of(snapshotConfig, result), receiver);

    verify(receiver, times(0)).output(Mockito.any());
  }

  /**
   * Tests that {@link CreateBigtableMutations#processElement} splits mutations into multiple Put
   * requests if the number of cells exceeds the threshold.
   */
  @Test
  public void testProcessElement_splitMutations() throws Exception {
    byte[] rowKey = "row-key".getBytes();
    Cell cell1 = new KeyValue(rowKey, "cf".getBytes(), "qual1".getBytes(), "val1".getBytes());
    Cell cell2 = new KeyValue(rowKey, "cf".getBytes(), "qual2".getBytes(), "val2".getBytes());

    List<Cell> cells = new ArrayList<>();
    cells.add(cell1);
    cells.add(cell2);

    when(result.listCells()).thenReturn(cells);
    when(result.getRow()).thenReturn(rowKey);

    CreateBigtableMutations fn =
        new CreateBigtableMutations(
            1, // maxMutationsPerRequestThreshold = 1
            false, 0L, false, 0, false, 0);

    fn.processElement(KV.of(snapshotConfig, result), receiver);

    ArgumentCaptor<KV<String, Iterable<Mutation>>> captor = ArgumentCaptor.forClass(KV.class);

    verify(receiver, times(1)).output(captor.capture());

    KV<String, Iterable<Mutation>> output = captor.getValue();
    assertNotNull(output);

    Iterator<Mutation> iterator = output.getValue().iterator();
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(2, count);
  }
}

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
package com.google.cloud.bigtable.beam.hbasesnapshots.coders;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableSchema;

/** Implementation of {@link Coder} for encoding and decoding of {@link RegionConfig} */
@InternalApi("For internal usage only")
public class RegionConfigCoder extends Coder<RegionConfig> {
  private static final VarLongCoder longCoder = VarLongCoder.of();

  private static final Coder<SnapshotConfig> snapshotConfigCoder =
      SerializableCoder.of(SnapshotConfig.class);
  private static final Coder<byte[]> byteArrayCoder = ByteArrayCoder.of();

  @Override
  public void encode(RegionConfig value, OutputStream outStream) throws IOException {
    snapshotConfigCoder.encode(value.getSnapshotConfig(), outStream);

    HBaseProtos.RegionInfo regionInfo = ProtobufUtil.toRegionInfo(value.getRegionInfo());
    byteArrayCoder.encode(regionInfo.toByteArray(), outStream);

    HBaseProtos.TableSchema tableSchema = ProtobufUtil.toTableSchema(value.getTableDescriptor());
    byteArrayCoder.encode(tableSchema.toByteArray(), outStream);

    longCoder.encode(value.getRegionSize(), outStream);
  }

  @Override
  public RegionConfig decode(InputStream inStream) throws IOException {
    SnapshotConfig snapshotConfig = snapshotConfigCoder.decode(inStream);

    byte[] regionInfoBytes = byteArrayCoder.decode(inStream);
    RegionInfo regionInfo =
        ProtobufUtil.toRegionInfo(HBaseProtos.RegionInfo.parseFrom(regionInfoBytes));

    byte[] tableSchemaBytes = byteArrayCoder.decode(inStream);
    TableDescriptor tableDescriptor =
        ProtobufUtil.toTableDescriptor(TableSchema.parseFrom(tableSchemaBytes));

    Long regionSize = longCoder.decode(inStream);

    return RegionConfig.builder()
        .setSnapshotConfig(snapshotConfig)
        .setRegionInfo(regionInfo)
        .setTableDescriptor(tableDescriptor)
        .setRegionSize(regionSize)
        .build();
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() throws Coder.NonDeterministicException {}
}

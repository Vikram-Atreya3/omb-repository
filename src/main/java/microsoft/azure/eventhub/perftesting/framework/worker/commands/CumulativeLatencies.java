/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package microsoft.azure.eventhub.perftesting.framework.worker.commands;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CumulativeLatencies {

  private static final Logger log = LoggerFactory.getLogger(CumulativeLatencies.class);

  public Boolean isSerializedObject = false;

  @JsonIgnore public Histogram publishLatency = new Histogram(TimeUnit.SECONDS.toMicros(600), 5);
  public byte[] publishLatencyBytes;

  @JsonIgnore public Histogram endToEndLatency = new Histogram(TimeUnit.HOURS.toMicros(12), 5);
  public byte[] endToEndLatencyBytes;

  public CumulativeLatencies plus(CumulativeLatencies toAdd) {
    CumulativeLatencies result = new CumulativeLatencies();

    // Deep copying itself to the new object
    result.publishLatency.add(this.publishLatency);
    result.endToEndLatency.add(this.endToEndLatency);

    if (toAdd.isSerializedObject) {
      try {
        result.publishLatency.add(
            Histogram.decodeFromCompressedByteBuffer(
                ByteBuffer.wrap(toAdd.publishLatencyBytes), TimeUnit.SECONDS.toMicros(600)));

        result.endToEndLatency.add(
            Histogram.decodeFromCompressedByteBuffer(
                ByteBuffer.wrap(toAdd.endToEndLatencyBytes), TimeUnit.HOURS.toMicros(12)));
      } catch (Exception e) {
        log.error("Failed to decode latency histograms for cumulative latencies.");
        throw new RuntimeException(e);
      }
    } else {
      result.publishLatency.add(toAdd.publishLatency);
      result.endToEndLatency.add(toAdd.endToEndLatency);
    }
    return result;
  }
}

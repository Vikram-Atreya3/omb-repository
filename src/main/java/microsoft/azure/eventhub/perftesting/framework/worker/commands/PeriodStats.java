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
import java.util.zip.DataFormatException;
import lombok.ToString;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ToString
public class PeriodStats {

  private static final Logger log = LoggerFactory.getLogger(PeriodStats.class);

  public Boolean isSerializedObject = false;

  public long messagesSent = 0;
  public long messageSendErrors = 0;
  public long bytesSent = 0;
  public long requestsSent = 0;

  public long messagesReceived = 0;
  public long bytesReceived = 0;

  public long totalMessagesSent = 0;
  public long totalMessageSendErrors = 0;
  public long totalMessagesReceived = 0;

  @JsonIgnore public Histogram publishLatency = new Histogram(TimeUnit.SECONDS.toMicros(600), 5);
  public byte[] publishLatencyBytes;

  @JsonIgnore public Histogram endToEndLatency = new Histogram(TimeUnit.HOURS.toMicros(12), 5);
  public byte[] endToEndLatencyBytes;

  public PeriodStats plus(PeriodStats toAdd) {
    PeriodStats result = new PeriodStats();
    // Deep copying itself to the new object
    result.messagesSent += this.messagesSent;
    result.requestsSent += this.requestsSent;
    result.messageSendErrors += this.messageSendErrors;
    result.bytesSent += this.bytesSent;
    result.messagesReceived += this.messagesReceived;
    result.bytesReceived += this.bytesReceived;
    result.totalMessagesSent += this.totalMessagesSent;
    result.totalMessageSendErrors += this.totalMessageSendErrors;
    result.totalMessagesReceived += this.totalMessagesReceived;
    result.publishLatency.add(this.publishLatency);
    result.endToEndLatency.add(this.endToEndLatency);

    result.messagesSent += toAdd.messagesSent;
    result.requestsSent += toAdd.requestsSent;
    result.messageSendErrors += toAdd.messageSendErrors;
    result.bytesSent += toAdd.bytesSent;
    result.messagesReceived += toAdd.messagesReceived;
    result.bytesReceived += toAdd.bytesReceived;
    result.totalMessagesSent += toAdd.totalMessagesSent;
    result.totalMessageSendErrors += toAdd.totalMessageSendErrors;
    result.totalMessagesReceived += toAdd.totalMessagesReceived;

    if (toAdd.isSerializedObject) {
      try {
        result.publishLatency.add(
            Histogram.decodeFromCompressedByteBuffer(
                ByteBuffer.wrap(toAdd.publishLatencyBytes), TimeUnit.SECONDS.toMicros(600)));

        result.endToEndLatency.add(
            Histogram.decodeFromCompressedByteBuffer(
                ByteBuffer.wrap(toAdd.endToEndLatencyBytes), TimeUnit.HOURS.toMicros(12)));
      } catch (ArrayIndexOutOfBoundsException | DataFormatException e) {
        log.error("Failed to decode latency histograms for period stats.");
        throw new RuntimeException(e);
      }
    } else {
      result.publishLatency.add(toAdd.publishLatency);
      result.endToEndLatency.add(toAdd.endToEndLatency);
    }
    return result;
  }
}

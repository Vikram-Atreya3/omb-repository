/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package microsoft.azure.eventhub.perftesting.framework.worker;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import microsoft.azure.eventhub.perftesting.framework.worker.commands.CountersStats;
import microsoft.azure.eventhub.perftesting.framework.worker.commands.CumulativeLatencies;
import microsoft.azure.eventhub.perftesting.framework.worker.commands.PeriodStats;
import org.HdrHistogram.Recorder;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

public class WorkerStats {
  public static final long HIGHEST_TRACKABLE_PUBLISH_VALUE = TimeUnit.SECONDS.toMicros(600);
  public static final long HIGHEST_TRACKABLE_E2E_VALUE = TimeUnit.HOURS.toMicros(12);
  private final StatsLogger statsLogger;

  private final LongAdder messagesSent = new LongAdder();
  private final LongAdder messageSendErrors = new LongAdder();
  private final LongAdder requestsSent = new LongAdder();
  private final LongAdder bytesSent = new LongAdder();
  private final LongAdder messagesReceived = new LongAdder();
  private final LongAdder bytesReceived = new LongAdder();

  private final Counter messageSendErrorCounter;
  private final Counter messagesSentCounter;
  private final Counter requestsSentCounter;
  private final Counter bytesSentCounter;
  private final Counter messagesReceivedCounter;
  private final Counter bytesReceivedCounter;

  private final LongAdder totalMessagesSent = new LongAdder();
  private final LongAdder totalMessageSendErrors = new LongAdder();
  private final LongAdder totalMessagesReceived = new LongAdder();

  private final Recorder publishLatencyRecorder = new Recorder(HIGHEST_TRACKABLE_PUBLISH_VALUE, 5);
  private final Recorder cumulativePublishLatencyRecorder =
      new Recorder(HIGHEST_TRACKABLE_PUBLISH_VALUE, 5);
  private final OpStatsLogger publishLatencyStats;

  private final Recorder endToEndLatencyRecorder = new Recorder(HIGHEST_TRACKABLE_E2E_VALUE, 5);
  private final Recorder endToEndCumulativeLatencyRecorder =
      new Recorder(HIGHEST_TRACKABLE_E2E_VALUE, 5);
  private final OpStatsLogger endToEndLatencyStats;

  WorkerStats(StatsLogger statsLogger) {
    this.statsLogger = statsLogger;

    StatsLogger producerStatsLogger = statsLogger.scope("producer");
    this.messagesSentCounter = producerStatsLogger.getCounter("messages_sent");
    this.requestsSentCounter = producerStatsLogger.getCounter("requests_sent");
    this.messageSendErrorCounter = producerStatsLogger.getCounter("message_send_errors");
    this.bytesSentCounter = producerStatsLogger.getCounter("bytes_sent");
    this.publishLatencyStats = producerStatsLogger.getOpStatsLogger("produce_latency");

    StatsLogger consumerStatsLogger = statsLogger.scope("consumer");
    this.messagesReceivedCounter = consumerStatsLogger.getCounter("messages_recv");
    this.bytesReceivedCounter = consumerStatsLogger.getCounter("bytes_recv");
    this.endToEndLatencyStats = consumerStatsLogger.getOpStatsLogger("e2e_latency");
  }

  public StatsLogger getStatsLogger() {
    return statsLogger;
  }

  public void recordMessageSent() {
    totalMessagesSent.increment();
  }

  public void recordMessageReceived(long payloadLength, long endToEndLatencyMicros) {
    messagesReceived.increment();
    totalMessagesReceived.increment();
    messagesReceivedCounter.inc();
    bytesReceived.add(payloadLength);
    bytesReceivedCounter.add(payloadLength);

    if (endToEndLatencyMicros > 0) {
      endToEndCumulativeLatencyRecorder.recordValue(endToEndLatencyMicros);
      endToEndLatencyRecorder.recordValue(endToEndLatencyMicros);
      endToEndLatencyStats.registerSuccessfulEvent(endToEndLatencyMicros, TimeUnit.MICROSECONDS);
    }
  }

  public PeriodStats toPeriodStats() {
    PeriodStats stats = new PeriodStats();

    stats.messagesSent = messagesSent.sumThenReset();
    stats.requestsSent = requestsSent.sumThenReset();
    stats.messageSendErrors = messageSendErrors.sumThenReset();
    stats.bytesSent = bytesSent.sumThenReset();

    stats.messagesReceived = messagesReceived.sumThenReset();
    stats.bytesReceived = bytesReceived.sumThenReset();

    stats.totalMessagesSent = totalMessagesSent.sum();
    stats.totalMessageSendErrors = totalMessageSendErrors.sum();
    stats.totalMessagesReceived = totalMessagesReceived.sum();

    stats.publishLatency = publishLatencyRecorder.getIntervalHistogram();
    stats.endToEndLatency = endToEndLatencyRecorder.getIntervalHistogram();
    return stats;
  }

  public CumulativeLatencies toCumulativeLatencies() {
    CumulativeLatencies latencies = new CumulativeLatencies();
    latencies.publishLatency = cumulativePublishLatencyRecorder.getIntervalHistogram();
    latencies.endToEndLatency = endToEndCumulativeLatencyRecorder.getIntervalHistogram();
    return latencies;
  }

  public CountersStats toCountersStats() throws IOException {
    CountersStats stats = new CountersStats();
    stats.messagesSent = totalMessagesSent.sum();
    stats.messageSendErrors = totalMessageSendErrors.sum();
    stats.messagesReceived = totalMessagesReceived.sum();
    return stats;
  }

  public void resetLatencies() {
    publishLatencyRecorder.reset();
    cumulativePublishLatencyRecorder.reset();
    endToEndLatencyRecorder.reset();
    endToEndCumulativeLatencyRecorder.reset();
  }

  public void reset() {
    resetLatencies();

    messagesSent.reset();
    requestsSent.reset();
    messageSendErrors.reset();
    bytesSent.reset();
    messagesReceived.reset();
    bytesReceived.reset();
    totalMessagesSent.reset();
    totalMessagesReceived.reset();

    bytesReceivedCounter.clear();
    bytesSentCounter.clear();
    messagesReceivedCounter.clear();
    messagesSentCounter.clear();
    requestsSentCounter.clear();
    messagesSentCounter.clear();
  }

  public void recordProducerFailure() {
    messageSendErrors.increment();
    messageSendErrorCounter.inc();
    totalMessageSendErrors.increment();
  }

  public void recordProducerSuccess(long msgSent, long payloadLength, long sendTimeNs, long nowNs) {
    requestsSent.increment();
    messagesSent.add(msgSent);
    totalMessagesSent.add(msgSent);
    bytesSent.add(payloadLength * msgSent);

    requestsSentCounter.inc();
    messagesSentCounter.add(msgSent);
    bytesSentCounter.add(payloadLength * msgSent);

    final long latencyMicros =
        Math.min(
            HIGHEST_TRACKABLE_PUBLISH_VALUE, TimeUnit.NANOSECONDS.toMicros(nowNs - sendTimeNs));
    publishLatencyRecorder.recordValue(latencyMicros);
    cumulativePublishLatencyRecorder.recordValue(latencyMicros);
    publishLatencyStats.registerSuccessfulEvent(latencyMicros, TimeUnit.MICROSECONDS);
  }
}

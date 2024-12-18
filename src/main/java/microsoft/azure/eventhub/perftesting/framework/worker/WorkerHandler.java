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
package microsoft.azure.eventhub.perftesting.framework.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.javalin.Context;
import io.javalin.Javalin;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import microsoft.azure.eventhub.perftesting.framework.driver.DriverConfiguration;
import microsoft.azure.eventhub.perftesting.framework.worker.commands.*;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class WorkerHandler {

  public static final String INITIALIZE_DRIVER = "/initialize-driver";
  public static final String HEALTH_CHECK = "/health-check";
  public static final String CREATE_TOPICS = "/create-topics";
  public static final String NOTIFY_TOPIC_CREATION = "/notify-topic-creation";
  public static final String CREATE_PRODUCERS = "/create-producers";
  public static final String PROBE_PRODUCERS = "/probe-producers";
  public static final String CREATE_CONSUMERS = "/create-consumers";
  public static final String PAUSE_CONSUMERS = "/pause-consumers";
  public static final String RESUME_CONSUMERS = "/resume-consumers";
  public static final String PAUSE_PRODUCERS = "/pause-producers";
  public static final String RESUME_PRODUCERS = "/resume-producers";
  public static final String START_LOAD = "/start-load";
  public static final String ADJUST_PUBLISH_RATE = "/adjust-publish-rate";
  public static final String STOP_ALL = "/stop-all";
  public static final String PERIOD_STATS = "/period-stats";
  public static final String CUMULATIVE_LATENCIES = "/cumulative-latencies";
  public static final String COUNTERS_STATS = "/counters-stats";
  public static final String RESET_STATS = "/reset-stats";
  private static final Logger log = LoggerFactory.getLogger(WorkerHandler.class);
  private static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

  static {
    mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
  }

  private final Worker localWorker;
  private final ByteBuffer histogramSerializationBuffer = ByteBuffer.allocate(1024 * 1024);

  public WorkerHandler(Javalin app, StatsLogger statsLogger) {
    this.localWorker = new LocalWorker(statsLogger);

    app.post(INITIALIZE_DRIVER, this::handleInitializeDriver);
    app.post(CREATE_TOPICS, this::handleCreateTopics);
    app.post(NOTIFY_TOPIC_CREATION, this::handleNotifyTopicCreation);
    app.post(CREATE_PRODUCERS, this::handleCreateProducers);
    app.post(PROBE_PRODUCERS, this::handleProbeProducers);
    app.post(CREATE_CONSUMERS, this::handleCreateConsumers);
    app.post(PAUSE_CONSUMERS, this::handlePauseConsumers);
    app.post(RESUME_CONSUMERS, this::handleResumeConsumers);
    app.post(PAUSE_PRODUCERS, this::handlePauseProducers);
    app.post(RESUME_PRODUCERS, this::handleResumeProducers);
    app.post(START_LOAD, this::handleStartLoad);
    app.post(ADJUST_PUBLISH_RATE, this::handleAdjustPublishRate);
    app.post(STOP_ALL, this::handleStopAll);
    app.get(PERIOD_STATS, this::handlePeriodStats);
    app.get(CUMULATIVE_LATENCIES, this::handleCumulativeLatencies);
    app.get(COUNTERS_STATS, this::handleCountersStats);
    app.post(RESET_STATS, this::handleResetStats);
    app.get(HEALTH_CHECK, this::healthCheck);

    app.exception(
        RuntimeException.class,
        (e, ctx) -> {
          log.error("Request handler: {} - Exception: {}", ctx.path(), e.getMessage());
          log.error("Caught Error" + Arrays.toString(e.getStackTrace()));
          ctx.status(HttpURLConnection.HTTP_INTERNAL_ERROR);
        });
  }

  private void healthCheck(Context ctx) throws Exception {
    ctx.result(writer.writeValueAsString("Service Healthy"));
  }

  private void handleInitializeDriver(Context ctx) throws Exception {
    localWorker.initializeDriver(
        (DriverConfiguration) SerializationUtils.deserialize(ctx.bodyAsBytes()));
    log.info("Completed Init of LocalDriver");
  }

  private void handleCreateTopics(Context ctx) throws Exception {
    TopicsInfo topicsInfo = mapper.readValue(ctx.body(), TopicsInfo.class);
    log.info("Received create topics request for topics: {}", ctx.body());
    List<Topic> topics = localWorker.createTopics(topicsInfo);
    ctx.result(writer.writeValueAsString(topics));
  }

  private void handleNotifyTopicCreation(Context ctx) throws Exception {
    List<Topic> topics = mapper.readValue(ctx.body(), new TypeReference<List<Topic>>() {});
    log.info("Received notify topic creation request for topics: {}", ctx.body());
    localWorker.notifyTopicCreation(topics);
  }

  private void handleCreateProducers(Context ctx) throws Exception {
    List<String> topics = (List<String>) mapper.readValue(ctx.body(), List.class);
    log.info("Received create producers request for topics: {}", topics);
    localWorker.createProducers(topics);
  }

  private void handleProbeProducers(Context ctx) throws Exception {
    localWorker.probeProducers();
  }

  private void handleCreateConsumers(Context ctx) throws Exception {
    ConsumerAssignment consumerAssignment = mapper.readValue(ctx.body(), ConsumerAssignment.class);

    log.info(
        "Received create consumers request for topics: {}", consumerAssignment.topicsSubscriptions);
    localWorker.createConsumers(consumerAssignment);
  }

  private void handlePauseConsumers(Context ctx) throws Exception {
    localWorker.pauseConsumers();
  }

  private void handleResumeConsumers(Context ctx) throws Exception {
    localWorker.resumeConsumers();
  }

  private void handlePauseProducers(Context ctx) throws Exception {
    localWorker.pauseProducers();
  }

  private void handleResumeProducers(Context ctx) throws Exception {
    localWorker.resumeProducers();
  }

  private void handleStartLoad(Context ctx) throws Exception {
    ProducerWorkAssignment producerWorkAssignment =
        mapper.readValue(ctx.body(), ProducerWorkAssignment.class);

    log.info(
        "Start load publish-rate: {} msg/s -- payload-size: {}",
        producerWorkAssignment.publishRate,
        producerWorkAssignment.payloadData.length);

    localWorker.startLoad(producerWorkAssignment);
  }

  private void handleAdjustPublishRate(Context ctx) throws Exception {
    Double publishRate = mapper.readValue(ctx.body(), Double.class);
    log.info("Adjust publish-rate: {} msg/s", publishRate);
    localWorker.adjustPublishRate(publishRate);
  }

  private void handleStopAll(Context ctx) throws Exception {
    log.info("Stop All");
    localWorker.stopAll();
    System.gc();
  }

  private void handlePeriodStats(Context ctx) throws Exception {
    PeriodStats stats = localWorker.getPeriodStats();

    // Serialize histograms
    synchronized (histogramSerializationBuffer) {
      histogramSerializationBuffer.clear();
      stats.publishLatency.encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
      stats.publishLatencyBytes = new byte[histogramSerializationBuffer.position()];
      histogramSerializationBuffer.flip();
      histogramSerializationBuffer.get(stats.publishLatencyBytes);

      histogramSerializationBuffer.clear();
      stats.endToEndLatency.encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
      stats.endToEndLatencyBytes = new byte[histogramSerializationBuffer.position()];
      histogramSerializationBuffer.flip();
      histogramSerializationBuffer.get(stats.endToEndLatencyBytes);
      stats.isSerializedObject = true;
    }

    ctx.result(writer.writeValueAsString(stats));
  }

  private void handleCumulativeLatencies(Context ctx) throws Exception {
    CumulativeLatencies stats = localWorker.getCumulativeLatencies();

    // Serialize histograms
    synchronized (histogramSerializationBuffer) {
      histogramSerializationBuffer.clear();
      stats.publishLatency.encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
      stats.publishLatencyBytes = new byte[histogramSerializationBuffer.position()];
      histogramSerializationBuffer.flip();
      histogramSerializationBuffer.get(stats.publishLatencyBytes);

      histogramSerializationBuffer.clear();
      stats.endToEndLatency.encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
      stats.endToEndLatencyBytes = new byte[histogramSerializationBuffer.position()];
      histogramSerializationBuffer.flip();
      histogramSerializationBuffer.get(stats.endToEndLatencyBytes);
      stats.isSerializedObject = true;
    }

    ctx.result(writer.writeValueAsString(stats));
  }

  private void handleCountersStats(Context ctx) throws Exception {
    ctx.result(writer.writeValueAsString(localWorker.getCountersStats()));
  }

  private void handleResetStats(Context ctx) throws Exception {
    log.info("Reset stats");
    localWorker.resetStats();
  }
}

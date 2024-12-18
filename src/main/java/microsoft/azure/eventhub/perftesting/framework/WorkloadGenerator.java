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
package microsoft.azure.eventhub.perftesting.framework;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import microsoft.azure.eventhub.perftesting.framework.pojo.inputs.BenchmarkingRunArguments;
import microsoft.azure.eventhub.perftesting.framework.pojo.inputs.Workload;
import microsoft.azure.eventhub.perftesting.framework.pojo.output.OMBMetrics;
import microsoft.azure.eventhub.perftesting.framework.pojo.output.SnapshotMetric;
import microsoft.azure.eventhub.perftesting.framework.pojo.output.TestDetails;
import microsoft.azure.eventhub.perftesting.framework.pojo.output.TestResult;
import microsoft.azure.eventhub.perftesting.framework.utils.PaddingDecimalFormat;
import microsoft.azure.eventhub.perftesting.framework.utils.RandomGenerator;
import microsoft.azure.eventhub.perftesting.framework.utils.Timer;
import microsoft.azure.eventhub.perftesting.framework.worker.Topic;
import microsoft.azure.eventhub.perftesting.framework.worker.Worker;
import microsoft.azure.eventhub.perftesting.framework.worker.commands.*;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WorkloadGenerator implements AutoCloseable {

  private static final DecimalFormat rateFormat = new PaddingDecimalFormat("0.000", 7);
  private static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.000", 4);
  private static final DecimalFormat dec = new PaddingDecimalFormat("0.000", 4);
  private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);
  private final BenchmarkingRunArguments arguments;
  private final Workload workload;
  private final Worker worker;
  private final String uniqueRunId;
  private final ProducerWorkAssignment producerWorkAssignment;
  private final ExecutorService executor =
      Executors.newCachedThreadPool(new DefaultThreadFactory("messaging-benchmark"));
  private volatile boolean runCompleted = false;
  private volatile boolean needToWaitForBacklogDraining = false;

  public WorkloadGenerator(BenchmarkingRunArguments arguments, Worker benchmarkingWorker) {
    this.arguments = arguments;
    this.workload = arguments.getWorkload();
    this.worker = benchmarkingWorker;
    this.uniqueRunId = arguments.getRunID();

    if (Optional.ofNullable(workload.consumerBacklogSizeGB).orElse(0L) > 0
        && workload.producerRate == 0) {
      throw new IllegalArgumentException(
          "Cannot probe producer sustainable rate when building backlog");
    }

    this.producerWorkAssignment =
        ProducerWorkAssignment.builder()
            .keyDistributorType(workload.keyDistributor)
            .payloadData(arguments.getMessagePayload().getPayloadData())
            .publishRate(workload.producerRate)
            .build();
  }

  public static double microsToMillis(double microTime) {
    return microTime / (1000);
  }

  public TestResult run() throws Exception {
    Timer timer = new Timer();
    final StringBuilder topicPrefixBuilder = new StringBuilder().append(arguments.getTestName());
    if (arguments.getTestSuiteName() != null)
      topicPrefixBuilder.append("-").append(arguments.getTestSuiteName());
    List<Topic> topics =
        worker.createTopics(
            new TopicsInfo(
                workload.topics,
                workload.partitionsPerTopic,
                workload.topicName,
                topicPrefixBuilder.toString()));
    log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());

    // Notify other workers about these topics
    worker.notifyTopicCreation(topics);

    if (workload.consumerPerSubscription > 0) {
      createConsumers(topics);
    }

    Thread.sleep(60000); // Adding a 1 min wait to avoid consumer lag affecting metrics

    createProducers(topics);

    worker.startLoad(producerWorkAssignment);

    /*
    //Commenting because we are not passing any consumer backlog
        if (Optional.ofNullable(workload.consumerBacklogSizeGB).orElse(0L) > 0) {
      executor.execute(
          () -> {
            try {
              buildAndDrainBacklog(topics);
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
    }
     */

    log.info("----- Starting warmup traffic ------");
    printAndCollectStats(workload.getWarmupTrafficDurationInMinutes(), TimeUnit.MINUTES);
    worker.resetStats();

    log.info("----- Starting benchmark traffic ------");
    TestResult result = printAndCollectStats(workload.testDurationMinutes, TimeUnit.MINUTES);
    runCompleted = true;

    try {
      worker.stopAll();
    } catch (IOException e) {
      log.error("Unable to stop workload - {}", e.toString());
    }
    return result;
  }

  private void ensureTopicsAreReady() throws IOException {
    log.info("Waiting for consumers to be ready");
    /*
     This is work around the fact that there's no way to have a consumer ready in Kafka without
     first publishing some message on the topic, which will then trigger the partition assignment to the consumers
    */

    int expectedMessages = workload.topics * workload.subscriptionsPerTopic;

    // In this case we just publish 1 message and then wait for consumers to receive the data
    worker.probeProducers();

    long start = System.currentTimeMillis();
    long end = start + 60 * 1000;
    while (System.currentTimeMillis() < end) {
      CountersStats stats = worker.getCountersStats();

      log.info(
          "Waiting for topics to be ready -- Sent: {}, Received: {}",
          stats.messagesSent,
          stats.messagesReceived);
      if (stats.messagesReceived < expectedMessages) {
        try {
          Thread.sleep(2_000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } else {
        break;
      }
    }

    if (System.currentTimeMillis() >= end) {
      throw new RuntimeException("Timed out waiting for consumers to be ready");
    } else {
      log.info("All consumers are ready");
    }
  }

  /**
   * Adjust the publish rate to a level that is sustainable, meaning that we can consume all the
   * messages that are being produced
   */
  private void findMaximumSustainableRate(double currentRate) throws IOException {
    double maxRate = Double.MAX_VALUE; // Discovered max sustainable rate
    double minRate = 0.1;

    CountersStats stats = worker.getCountersStats();

    long localTotalMessagesSentCounter = stats.messagesSent;
    long localTotalMessagesReceivedCounter = stats.messagesReceived;

    int controlPeriodMillis = 3000;
    long lastControlTimestamp = System.nanoTime();

    int successfulPeriods = 0;

    while (!runCompleted) {
      // Check every few seconds and adjust the rate
      try {
        Thread.sleep(controlPeriodMillis);
      } catch (InterruptedException e) {
        return;
      }

      // Consider multiple copies when using multiple subscriptions
      stats = worker.getCountersStats();
      long currentTime = System.nanoTime();
      long totalMessagesSent = stats.messagesSent;
      long totalMessagesReceived = stats.messagesReceived;
      long messagesPublishedInPeriod = totalMessagesSent - localTotalMessagesSentCounter;
      long messagesReceivedInPeriod = totalMessagesReceived - localTotalMessagesReceivedCounter;
      double publishRateInLastPeriod =
          messagesPublishedInPeriod
              / (double) (currentTime - lastControlTimestamp)
              * TimeUnit.SECONDS.toNanos(1);
      double receiveRateInLastPeriod =
          messagesReceivedInPeriod
              / (double) (currentTime - lastControlTimestamp)
              * TimeUnit.SECONDS.toNanos(1);

      if (log.isDebugEnabled()) {
        log.debug(
            "total-send: {} -- total-received: {} -- int-sent: {} -- int-received: {} -- sent-rate: {} -- received-rate: {}",
            totalMessagesSent,
            totalMessagesReceived,
            messagesPublishedInPeriod,
            messagesReceivedInPeriod,
            publishRateInLastPeriod,
            receiveRateInLastPeriod);
      }

      localTotalMessagesSentCounter = totalMessagesSent;
      localTotalMessagesReceivedCounter = totalMessagesReceived;
      lastControlTimestamp = currentTime;

      if (log.isDebugEnabled()) {
        log.debug(
            "Current rate: {} -- Publish rate {} -- Consume Rate: {} -- min-rate: {} -- max-rate: {}",
            dec.format(currentRate),
            dec.format(publishRateInLastPeriod),
            dec.format(receiveRateInLastPeriod),
            dec.format(minRate),
            dec.format(maxRate));
      }

      if (publishRateInLastPeriod < currentRate * 0.95) {
        // Producer is not able to publish as fast as requested
        maxRate = currentRate * 1.1;
        currentRate = minRate + (currentRate - minRate) / 2;

        log.debug("Publishers are not meeting requested rate. reducing to {}", currentRate);
      } else if (receiveRateInLastPeriod < publishRateInLastPeriod * 0.98) {
        // If the consumers are building backlog, we should slow down publish rate
        maxRate = currentRate;
        currentRate = minRate + (currentRate - minRate) / 2;
        log.debug("Consumers are not meeting requested rate. reducing to {}", currentRate);

        // Slows the publishes to let the consumer time to absorb the backlog
        worker.adjustPublishRate(minRate / 10);
        while (true) {
          stats = worker.getCountersStats();
          long backlog =
              workload.subscriptionsPerTopic * stats.messagesSent - stats.messagesReceived;
          if (backlog < 1000) {
            break;
          }

          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            return;
          }
        }

        log.debug("Resuming load at reduced rate");
        worker.adjustPublishRate(currentRate);

        try {
          // Wait some more time for the publish rate to catch up
          Thread.sleep(500);
        } catch (InterruptedException e) {
          return;
        }

        stats = worker.getCountersStats();
        localTotalMessagesSentCounter = stats.messagesSent;
        localTotalMessagesReceivedCounter = stats.messagesReceived;

      } else if (currentRate < maxRate) {
        minRate = currentRate;
        currentRate = Math.min(currentRate * 2, maxRate);
        log.debug("No bottleneck found, increasing the rate to {}", currentRate);
      } else if (++successfulPeriods > 3) {
        minRate = currentRate * 0.95;
        maxRate = currentRate * 1.05;
        successfulPeriods = 0;
      }

      worker.adjustPublishRate(currentRate);
    }
  }

  @Override
  public void close() throws Exception {
    worker.stopAll();
    executor.shutdownNow();
  }

  private void createConsumers(List<Topic> topics) throws IOException {
    ConsumerAssignment consumerAssignment = new ConsumerAssignment();

    for (Topic topic : topics) {
      for (int i = 0; i < workload.subscriptionsPerTopic; i++) {
        String subscriptionName =
            String.format("sub-%03d-%s", i, RandomGenerator.getRandomString());
        for (int j = 0; j < workload.consumerPerSubscription; j++) {
          consumerAssignment.topicsSubscriptions.add(
              new TopicSubscription(topic.name, subscriptionName, j));
        }
      }
    }

    Collections.shuffle(consumerAssignment.topicsSubscriptions);

    Timer timer = new Timer();

    worker.createConsumers(consumerAssignment);
    log.info(
        "Created {} consumers in {} ms",
        consumerAssignment.topicsSubscriptions.size(),
        timer.elapsedMillis());
  }

  private void createProducers(List<Topic> topics) throws IOException {
    List<String> fullListOfTopics = new ArrayList<>();

    // Add the topic multiple times, one for each producer
    for (int i = 0; i < workload.producersPerTopic; i++) {
      topics.forEach(topic -> fullListOfTopics.add(topic.name));
    }

    Collections.shuffle(fullListOfTopics);

    Timer timer = new Timer();

    worker.createProducers(fullListOfTopics);
    log.info("Created {} producers in {} ms", fullListOfTopics.size(), timer.elapsedMillis());
  }

  private void buildAndDrainBacklog(List<Topic> topics) throws IOException {
    log.info("Stopping all consumers to build backlog");
    worker.pauseConsumers();

    this.needToWaitForBacklogDraining = true;

    long requestedBacklogSize =
        Optional.ofNullable(workload.consumerBacklogSizeGB).orElse(0L) * 1024 * 1024 * 1024;

    while (true) {
      CountersStats stats = worker.getCountersStats();
      long currentBacklogSize =
          (workload.subscriptionsPerTopic * stats.messagesSent - stats.messagesReceived)
              * workload.messageSize;

      if (currentBacklogSize >= requestedBacklogSize) {
        break;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    drainBacklog(1000000);
  }

  private void drainBacklog(long waitTimeInMs) throws IOException {
    log.info("--- Start draining backlog ---");
    worker.pauseProducers();
    worker.resumeConsumers();

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    final long minBacklog = 100;
    long currentBacklog = 0;
    while (stopWatch.getTime(TimeUnit.MILLISECONDS) < waitTimeInMs) {
      CountersStats stats = worker.getCountersStats();
      currentBacklog = workload.subscriptionsPerTopic * stats.messagesSent - stats.messagesReceived;
      if (currentBacklog <= minBacklog) {
        log.info("--- Completed backlog draining ---");
        needToWaitForBacklogDraining = false;
        worker.resumeProducers();
        return;
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    log.info(
        "Returning due to time spent during backlog draining. Current Backlog = " + currentBacklog);
    worker.resumeProducers();
  }

  private TestResult printAndCollectStats(long testDurations, TimeUnit unit) throws IOException {
    long startTime = System.nanoTime();

    // Print report stats
    long oldTime = System.nanoTime();

    long testEndTime = testDurations > 0 ? startTime + unit.toNanos(testDurations) : Long.MAX_VALUE;

    TestResult result = new TestResult();
    result.testDetails = new TestDetails();

    result.testDetails.runID = this.uniqueRunId;
    result.testDetails.testStartTime =
        Instant.now().truncatedTo(ChronoUnit.SECONDS).toString().replaceAll("[TZ]", " ");
    result.testDetails.testRunDurationInMinutes = TimeUnit.MINUTES.convert(testDurations, unit);
    OMBMetrics aggregateResult = OMBMetrics.builder().uuid(this.uniqueRunId).build();

    while (true) {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        break;
      }

      PeriodStats stats = worker.getPeriodStats();

      long now = System.nanoTime();
      double elapsed = (now - oldTime) / 1e9;

      double publishRate = stats.messagesSent / elapsed;
      double publishThroughput = stats.bytesSent / elapsed / 1024 / 1024;
      double requestRate = stats.requestsSent / elapsed;
      double errorRate = stats.messageSendErrors / elapsed;

      double consumeRate = stats.messagesReceived / elapsed;
      double consumeThroughput = stats.bytesReceived / elapsed / 1024 / 1024;

      long currentBacklog =
          workload.subscriptionsPerTopic * stats.totalMessagesSent - stats.totalMessagesReceived;

      log.info(
          "Pub rate {} msg/s / {} Mb/s / {} Req/s | Pub err {} err/s  | Cons rate {} msg/s / {} Mb/s | Backlog: {} K ",
          rateFormat.format(publishRate),
          throughputFormat.format(publishThroughput),
          rateFormat.format(requestRate),
          rateFormat.format(errorRate),
          rateFormat.format(consumeRate),
          throughputFormat.format(consumeThroughput),
          dec.format(currentBacklog / 1000.0));

      log.info(
          "Pub Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {}",
          dec.format(microsToMillis(stats.publishLatency.getMean())),
          dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(50))),
          dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(99))),
          dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(99.9))),
          throughputFormat.format(microsToMillis(stats.publishLatency.getMaxValue())));

      log.info(
          "E2E Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {}",
          dec.format(microsToMillis(stats.endToEndLatency.getMean())),
          dec.format(microsToMillis(stats.endToEndLatency.getValueAtPercentile(50))),
          dec.format(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99))),
          dec.format(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.9))),
          throughputFormat.format(microsToMillis(stats.endToEndLatency.getMaxValue())));

      SnapshotMetric snapshotMetric =
          SnapshotMetric.builder()
              .uuid(this.uniqueRunId)
              .timestamp(
                  Instant.now().truncatedTo(ChronoUnit.SECONDS).toString().replaceAll("[TZ]", " "))
              .timeSinceTestStartInSeconds(TimeUnit.NANOSECONDS.toSeconds(now - startTime))
              .publishRate(Precision.round(publishRate, 2))
              .consumeRate(Precision.round(consumeRate, 2))
              .publishErrorRate(Precision.round(errorRate, 2))
              .backlog(currentBacklog)
              .build();

      snapshotMetric.latencyMetric.populatePublishLatency(stats.publishLatency);
      snapshotMetric.latencyMetric.populateE2ELatency(stats.endToEndLatency);
      aggregateResult.calculateMovingAverage(snapshotMetric, result.getSnapshotMetrics().size());
      result.snapshotMetrics.add(snapshotMetric);

      if (now >= testEndTime && !needToWaitForBacklogDraining) {
        boolean complete = false;
        int retry = 0;
        CumulativeLatencies agg = null;
        do {
          try {
            agg = worker.getCumulativeLatencies();
          } catch (Exception e) {
            log.info("Retrying");
            retry++;
            continue;
          }
          complete = true;
        } while (!complete && retry < 10);

        if (!complete) {
          throw new RuntimeException("Failed to collect aggregate latencies");
        }

        log.info(
            "----- Aggregated Pub Latency (ms) avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {} -----",
            dec.format(microsToMillis(agg.publishLatency.getMean())),
            dec.format(microsToMillis(agg.publishLatency.getValueAtPercentile(50))),
            dec.format(microsToMillis(agg.publishLatency.getValueAtPercentile(95))),
            dec.format(microsToMillis(agg.publishLatency.getValueAtPercentile(99))),
            dec.format(microsToMillis(agg.publishLatency.getValueAtPercentile(99.9))),
            dec.format(microsToMillis(agg.publishLatency.getValueAtPercentile(99.99))),
            throughputFormat.format(microsToMillis(agg.publishLatency.getMaxValue())));

        log.info(
            "----- Aggregated E2E Latency (ms) avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {} -----",
            dec.format(microsToMillis(agg.endToEndLatency.getMean())),
            dec.format(microsToMillis(agg.endToEndLatency.getValueAtPercentile(50))),
            dec.format(microsToMillis(agg.endToEndLatency.getValueAtPercentile(95))),
            dec.format(microsToMillis(agg.endToEndLatency.getValueAtPercentile(99))),
            dec.format(microsToMillis(agg.endToEndLatency.getValueAtPercentile(99.9))),
            dec.format(microsToMillis(agg.endToEndLatency.getValueAtPercentile(99.99))),
            throughputFormat.format(microsToMillis(agg.endToEndLatency.getMaxValue())));

        aggregateResult.setTimestamp(
            Instant.now().truncatedTo(ChronoUnit.SECONDS).toString().replaceAll("[TZ]", " "));
        aggregateResult.latencyMetric.populatePublishLatency(agg.publishLatency);
        aggregateResult.latencyMetric.populateE2ELatency(agg.endToEndLatency);
        result.aggregateResult = aggregateResult;

        break;
      }

      oldTime = now;
    }
    return result;
  }
}

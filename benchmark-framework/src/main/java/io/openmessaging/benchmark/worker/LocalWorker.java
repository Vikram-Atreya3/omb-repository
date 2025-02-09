/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.worker;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.openmessaging.benchmark.driver.*;
import io.openmessaging.benchmark.utils.RandomGenerator;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.distributor.KeyDistributor;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.*;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalWorker implements Worker, ConsumerCallback {
    private final RateLimiter rateLimiter = RateLimiter.create(1.0);
    private final ExecutorService executor = Executors.newCachedThreadPool(new DefaultThreadFactory("local-worker"));
    private final WorkerStats stats;
    private BenchmarkDriver benchmarkDriver = null;
    private List<BenchmarkProducer> producers = new ArrayList<>();
    private List<BenchmarkConsumer> consumers = new ArrayList<>();
    private boolean testCompleted = false;
    private boolean consumersArePaused = false;
    private boolean producersArePaused = false;

    public LocalWorker() {
        this(NullStatsLogger.INSTANCE);
    }

    public LocalWorker(StatsLogger statsLogger) {
        stats = new WorkerStats(statsLogger);
    }

    @Override
    public void initializeDriver(File driverConfigFile) throws IOException {
        Preconditions.checkArgument(benchmarkDriver == null);
        testCompleted = false;

        DriverConfiguration driverConfiguration = mapper.readValue(driverConfigFile, DriverConfiguration.class);

        log.info("Driver: {}", writer.writeValueAsString(driverConfiguration));

        try {
            benchmarkDriver = (BenchmarkDriver) Class.forName(driverConfiguration.driverClass).newInstance();
            benchmarkDriver.initialize(driverConfigFile, stats.getStatsLogger());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Topic> createTopics(TopicsInfo topicsInfo) {
        Preconditions.checkArgument(topicsInfo.numberOfTopics > 0, "Number of Topics have to be non zero");
        if (StringUtils.isNotEmpty(topicsInfo.topicName)) {
            Preconditions.checkArgument(topicsInfo.numberOfTopics == 1, "Can't specify multiple topics when specifying topic name");
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        Timer timer = new Timer();
        List<Topic> topics = new ArrayList<>();
        for (int i = 0; i < topicsInfo.numberOfTopics; i++) {
            String topicName = Optional.ofNullable(topicsInfo.topicName)
                    .orElse(String.format("%s-%s-%04d", benchmarkDriver.getTopicNamePrefix(), RandomGenerator.getRandomString(), i));
            Topic topic = new Topic(topicName,topicsInfo.numberOfPartitionsPerTopic);
            topics.add(topic);
            futures.add(benchmarkDriver.createTopic(topic.name, topic.partitions));
        }

        futures.forEach(CompletableFuture::join);

        log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());
        return topics;
    }

    @Override
    public void notifyTopicCreation(List<Topic> topics) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Topic topic : topics) {
            futures.add(benchmarkDriver.notifyTopicCreation(topic.name, topic.partitions));
        }

        futures.forEach(CompletableFuture::join);
    }

    @Override
    public void createProducers(List<String> topics) {
        Timer timer = new Timer();

        List<CompletableFuture<BenchmarkProducer>> futures = topics.stream()
                .map(topic -> benchmarkDriver.createProducer(topic)).collect(toList());

        futures.forEach(f -> producers.add(f.join()));
        log.info("Created {} producers in {} ms", producers.size(), timer.elapsedMillis());
    }

    @Override
    public void createConsumers(ConsumerAssignment consumerAssignment) {
        Timer timer = new Timer();

        List<CompletableFuture<BenchmarkConsumer>> futures = consumerAssignment.topicsSubscriptions.stream()
                .map(ts -> benchmarkDriver.createConsumer(ts.topic, ts.subscription, Optional.of(ts.partition), this))
                .collect(toList());

        futures.forEach(f -> consumers.add(f.join()));
        log.info("Created {} consumers in {} ms", consumers.size(), timer.elapsedMillis());
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
        int processors = Runtime.getRuntime().availableProcessors();

        rateLimiter.setRate(producerWorkAssignment.publishRate);

        Map<Integer, List<BenchmarkProducer>> processorAssignemnt = new TreeMap<>();

        int processorIdx = 0;
        for (BenchmarkProducer p : producers) {
            processorAssignemnt.computeIfAbsent(processorIdx, x -> new ArrayList<BenchmarkProducer>()).add(p);

            processorIdx = (processorIdx + 1) % processors;
        }

        processorAssignemnt.values().forEach(producers -> submitProducersToExecutor(producers,
                KeyDistributor.build(producerWorkAssignment.keyDistributorType), producerWorkAssignment.payloadData));
    }

    @Override
    public void probeProducers() throws IOException {
        producers.forEach(
                producer ->
                        producer.sendAsync(Optional.of("key"), new byte[10]).thenRun(stats::recordMessageSent));
    }

    private void submitProducersToExecutor(List<BenchmarkProducer> producers, KeyDistributor keyDistributor,
            byte[] payloadData) {
        MessageProducer messageProducer = new MessageProducer(rateLimiter, stats);
        executor.submit(() -> {
            try {
                while (!testCompleted) {
                    while (producersArePaused) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    producers.forEach(producer -> {
                        messageProducer.sendMessage(producer, keyDistributor.next(), payloadData);
                    });
                }
            } catch (Throwable t) {
                log.error("Got error", t);
            }
        });
    }

    @Override
    public void adjustPublishRate(double publishRate) {
        if (publishRate < 1.0) {
            rateLimiter.setRate(1.0);
            return;
        }
        rateLimiter.setRate(publishRate);
    }

    @Override
    public PeriodStats getPeriodStats() {
        return stats.toPeriodStats();
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        return stats.toCumulativeLatencies();
    }

    @Override
    public CountersStats getCountersStats() throws IOException {
        return stats.toCountersStats();
    }

    @Override
    public void messageReceived(byte[] data, long publishTimestamp) {
        internalMessageReceived(data.length, publishTimestamp);
    }

    public void internalMessageReceived(int size, long publishTimestampNanos) {
        long currentTimeNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        long endToEndLatencyMicros = TimeUnit.NANOSECONDS.toMicros(currentTimeNanos - publishTimestampNanos);
        stats.recordMessageReceived(size, endToEndLatencyMicros);

        while (consumersArePaused) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void pauseConsumers() throws IOException {
        consumersArePaused = true;
        log.info("Pausing consumers");
    }

    @Override
    public void resumeConsumers() throws IOException {
        consumersArePaused = false;
        log.info("Resuming consumers");
    }

    @Override
    public void pauseProducers() throws IOException {
        producersArePaused = true;
        log.info("Pausing producers");
    }

    @Override
    public void resumeProducers() throws IOException {
        producersArePaused = false;
        log.info("Resuming producers");
    }

    @Override
    public void healthCheck() throws IOException {
        log.info("Service Healthy");
    }

    @Override
    public void resetStats() throws IOException {
        stats.reset();
    }

    @Override
    public void stopAll() throws IOException {
        testCompleted = true;
        consumersArePaused = false;
        producersArePaused = false;
        stats.reset();

        try {
            Thread.sleep(100);
            for (BenchmarkProducer producer : producers) {
                producer.close();
            }
            producers.clear();

            for (BenchmarkConsumer consumer : consumers) {
                consumer.close();
            }
            consumers.clear();

            if (benchmarkDriver != null) {
                benchmarkDriver.close();
                benchmarkDriver = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final Logger log = LoggerFactory.getLogger(LocalWorker.class);
}

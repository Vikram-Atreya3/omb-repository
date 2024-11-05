/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.driver.eventhubs;

import com.azure.core.credential.TokenCredential;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.implementation.EventHubSharedKeyCredential;
import com.azure.resourcemanager.eventhubs.models.EventHub;
import com.azure.resourcemanager.eventhubs.models.EventHubNamespaceAuthorizationRule;
import com.azure.storage.blob.BlobContainerAsyncClient;
import io.openmessaging.benchmark.appconfig.adapter.ConfigProvider;
import io.openmessaging.benchmark.appconfig.adapter.ConfigurationKey;
import io.openmessaging.benchmark.driver.*;
import io.openmessaging.benchmark.storage.adapter.StorageAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class EventHubsBenchmarkDriver implements BenchmarkDriver {

    private static final Logger log = LoggerFactory.getLogger(EventHubsBenchmarkDriver.class);

    private String topicPrefix;
    protected String namespace;
    protected TokenCredential credential;
    protected Properties producerProperties;

    protected final List<BenchmarkProducer> producers = Collections.synchronizedList(new ArrayList<>());
    private final List<BenchmarkConsumer> consumers = Collections.synchronizedList(new ArrayList<>());
    private BlobContainerAsyncClient blobContainerAsyncClient;
    private EventHubAdministrator eventHubAdministrator;
    protected ConfigProvider configProvider;

    @Override
    public void initialize(DriverConfiguration driverConfiguration) throws IOException {
        configProvider = ConfigProvider.getInstance();

        log.info("Initializing "+ this.getClass().getSimpleName() + " with configuration " +  driverConfiguration.name);
        log.info("Using Namespace for this test run- " + driverConfiguration.namespaceMetadata.namespaceName);

        Properties commonProperties = new Properties();
        commonProperties.load(new StringReader(driverConfiguration.commonConfig));

        producerProperties = new Properties();
        producerProperties.putAll(commonProperties);
        producerProperties.load(new StringReader(driverConfiguration.producerConfig));
        producerProperties.putIfAbsent("batch.size", "1048576");
        producerProperties.putIfAbsent("batch.count", "1");

        Properties topicProperties = new Properties();
        topicProperties.load(new StringReader(driverConfiguration.topicConfig));

        topicPrefix = topicProperties.getProperty("topic.name.prefix");
        namespace = driverConfiguration.namespaceMetadata.namespaceName;

        blobContainerAsyncClient = StorageAdapter.GetAsyncStorageClient(configProvider.getConfigurationValue(ConfigurationKey.StorageAccountName),
                configProvider.getConfigurationValue(ConfigurationKey.StorageContainerName));
        eventHubAdministrator = new EventHubAdministrator(driverConfiguration.namespaceMetadata);

        if (driverConfiguration.reset) {
            String resourceGroup = driverConfiguration.namespaceMetadata.resourceGroup;
            log.info("Deleting existing entities");
            for (EventHub eh : eventHubAdministrator.listTopicForNamespace()) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        eventHubAdministrator.deleteTopic(eh.name());
                        log.info("Successfully deleted entity "+ eh.name());
                    }
                }).start();
            }
        }

        if (driverConfiguration.namespaceMetadata.sasKeyValue == null) {
            //Fetch details from Management APIs
            final EventHubNamespaceAuthorizationRule authorizationRule = eventHubAdministrator.getAuthorizationRule();
            driverConfiguration.namespaceMetadata.sasKeyName = authorizationRule.name();
            driverConfiguration.namespaceMetadata.sasKeyValue = authorizationRule.getKeys().primaryKey();
        }

        credential =  new EventHubSharedKeyCredential(driverConfiguration.namespaceMetadata.sasKeyName, driverConfiguration.namespaceMetadata.sasKeyValue);
    }

    @Override
    public String getTopicNamePrefix() {
        return topicPrefix;
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        return CompletableFuture.runAsync(() -> eventHubAdministrator.createTopic(topic, partitions));
    }

    @Override
    public CompletableFuture<Void> notifyTopicCreation(String topic, int partitions) {
        //NO-OP
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        EventHubProducerAsyncClient ehProducerClient = new EventHubClientBuilder()
                .credential(namespace + configProvider.getConfigurationValue(ConfigurationKey.FQDNSuffix), topic, credential)
                .buildAsyncProducerClient();
        BenchmarkProducer benchmarkProducer = new EventHubsBenchmarkProducer(ehProducerClient, producerProperties);
        try {
            producers.add(benchmarkProducer);
            return CompletableFuture.completedFuture(benchmarkProducer);
        } catch (Exception e) {
            ehProducerClient.close();
            CompletableFuture<BenchmarkProducer> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName,
                                                               Optional<Integer> partition,
                                                               ConsumerCallback consumerCallback) {

        eventHubAdministrator.createConsumerGroupIfNotPresent(topic, subscriptionName);

        EventProcessorClient eventProcessorClient = new EventProcessorClientBuilder()
                .credential(namespace + configProvider.getConfigurationValue(ConfigurationKey.FQDNSuffix), topic, credential)
                .consumerGroup(subscriptionName)
                .processEvent(eventContext -> EventHubsBenchmarkConsumer.processEvent(eventContext, consumerCallback))
                .processError(errorContext -> log.error("exception occur while consuming message " +  errorContext.getThrowable().getMessage()))
                .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient))
                .buildEventProcessorClient();

        try {
            BenchmarkConsumer benchmarkConsumer = new EventHubsBenchmarkConsumer(eventProcessorClient);
            consumers.add(benchmarkConsumer);
            return CompletableFuture.completedFuture(benchmarkConsumer);
        } catch (Throwable t) {
            eventProcessorClient.stop();
            CompletableFuture<BenchmarkConsumer> future = new CompletableFuture<>();
            future.completeExceptionally(t);
            return future;
        }
    }


    @Override
    public void close() throws Exception {
        for (BenchmarkProducer producer : producers) {
            producer.close();
        }

        for (BenchmarkConsumer consumer : consumers) {
            consumer.close();
        }
    }
}

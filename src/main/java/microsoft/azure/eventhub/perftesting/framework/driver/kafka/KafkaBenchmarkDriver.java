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
package microsoft.azure.eventhub.perftesting.framework.driver.kafka;

import com.azure.resourcemanager.eventhubs.models.EventHubNamespaceAuthorizationRule;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import microsoft.azure.eventhub.perftesting.framework.appconfigadapter.ConfigProvider;
import microsoft.azure.eventhub.perftesting.framework.appconfigadapter.ConfigurationKey;
import microsoft.azure.eventhub.perftesting.framework.driver.DriverConfiguration;
import microsoft.azure.eventhub.perftesting.framework.driver.EventHubAdministrator;
import microsoft.azure.eventhub.perftesting.framework.driver.base.BenchmarkConsumer;
import microsoft.azure.eventhub.perftesting.framework.driver.base.BenchmarkDriver;
import microsoft.azure.eventhub.perftesting.framework.driver.base.BenchmarkProducer;
import microsoft.azure.eventhub.perftesting.framework.driver.base.ConsumerCallback;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaBenchmarkDriver implements BenchmarkDriver {
  private static final Logger log = LoggerFactory.getLogger(KafkaBenchmarkDriver.class);
  private final List<BenchmarkProducer> producers = Collections.synchronizedList(new ArrayList<>());
  private final List<BenchmarkConsumer> consumers = Collections.synchronizedList(new ArrayList<>());
  private DriverConfiguration kafkaDriverConfig;
  private Properties topicProperties;
  private Properties producerProperties;
  private Properties consumerProperties;

  private AdminClient admin;

  @Override
  public void initialize(DriverConfiguration driverConfiguration) throws IOException {
    this.kafkaDriverConfig = driverConfiguration;
    ConfigProvider configProvider = ConfigProvider.getInstance();

    EventHubAdministrator eventHubAdministrator =
        EventHubAdministrator.getInstance(driverConfiguration.namespaceMetadata);

    if (kafkaDriverConfig.namespaceMetadata.sasKeyValue == null) {
      // Fetch details from EH Management APIs
      final EventHubNamespaceAuthorizationRule authorizationRule =
          eventHubAdministrator.getAuthorizationRule();
      kafkaDriverConfig.namespaceMetadata.sasKeyName = authorizationRule.name();
      kafkaDriverConfig.namespaceMetadata.sasKeyValue = authorizationRule.getKeys().primaryKey();
    }

    log.info(
        "Initializing "
            + this.getClass().getSimpleName()
            + " with configuration "
            + kafkaDriverConfig.name);
    log.info(
        "Using Namespace for this test run- " + kafkaDriverConfig.namespaceMetadata.namespaceName);

    Properties commonProperties = new Properties();
    commonProperties.load(new StringReader(kafkaDriverConfig.commonConfig));

    // manually creating bootstrap server from namespace name for Kafka
    commonProperties.put(
        "bootstrap.servers",
        kafkaDriverConfig.namespaceMetadata.namespaceName
            + configProvider.getConfigurationValue(ConfigurationKey.FQDNSuffix)
            + ":9093");

    // creating sasl driverConfiguration string from connection string
    final String jaasConfig = commonProperties.getProperty("sasl.jaas.config");
    commonProperties.put(
        "sasl.jaas.config",
        jaasConfig
            + "\""
            + createEventHubConnectionString(
                kafkaDriverConfig.namespaceMetadata.namespaceName,
                configProvider.getConfigurationValue(ConfigurationKey.FQDNSuffix),
                kafkaDriverConfig.namespaceMetadata.sasKeyName,
                kafkaDriverConfig.namespaceMetadata.sasKeyValue)
            + "\";");

    producerProperties = new Properties();
    producerProperties.putAll(commonProperties);
    producerProperties.load(new StringReader(kafkaDriverConfig.producerConfig));
    producerProperties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    consumerProperties = new Properties();
    consumerProperties.putAll(commonProperties);
    consumerProperties.load(new StringReader(kafkaDriverConfig.consumerConfig));
    consumerProperties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    topicProperties = new Properties();
    topicProperties.load(new StringReader(kafkaDriverConfig.topicConfig));

    try {
      admin = AdminClient.create(commonProperties);
    } catch (Exception e) {
      log.error("Failed to create Kafka Admin Client", e);
      throw e;
    }

    if (Optional.ofNullable(kafkaDriverConfig.reset).orElse(true)) {
      try {
        // List existing topics
        ListTopicsResult result = admin.listTopics();
        Set<String> topics = result.names().get();
        log.info("Deleting the following topics - {}", topics);
        // Delete all existing topics
        DeleteTopicsResult deletes = admin.deleteTopics(topics);
        try {
          deletes.all().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
          log.warn("Got error while deleting topic - {}", e.getMessage());
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new IOException(e);
      }
    }
  }

  @Override
  public String getTopicNamePrefix() {
    return "test-topic-kafka";
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public CompletableFuture<Void> createTopic(String topic, int partitions) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            final List<String> existingTopics =
                admin.listTopics().names().get().stream()
                    .map(s -> s.toLowerCase(Locale.ROOT))
                    .collect(Collectors.toList());
            if (!existingTopics.contains(topic.toLowerCase(Locale.ROOT))) {
              NewTopic newTopic =
                  new NewTopic(topic, partitions, kafkaDriverConfig.replicationFactor);
              newTopic.configs(new HashMap<>((Map) topicProperties));
              admin.createTopics(Arrays.asList(newTopic)).all().get();
              log.info(" Creating Topic Name: " + topic);
            } else {
              log.info("Reusing Topic " + topic + "as it already exists");
            }
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public CompletableFuture<Void> notifyTopicCreation(String topic, int partitions) {
    // No-op
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
    KafkaProducer<String, byte[]> kafkaProducer = new KafkaProducer<>(producerProperties);
    BenchmarkProducer benchmarkProducer = new KafkaBenchmarkProducer(kafkaProducer, topic);
    try {
      // Add to producer list to close later
      producers.add(benchmarkProducer);
      return CompletableFuture.completedFuture(benchmarkProducer);
    } catch (Throwable t) {
      kafkaProducer.close();
      CompletableFuture<BenchmarkProducer> future = new CompletableFuture<>();
      future.completeExceptionally(t);
      return future;
    }
  }

  @Override
  public CompletableFuture<BenchmarkConsumer> createConsumer(
      String topic,
      String subscriptionName,
      Optional<Integer> partition,
      ConsumerCallback consumerCallback) {
    Properties properties = new Properties();
    consumerProperties.forEach((key, value) -> properties.put(key, value));
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, subscriptionName);
    KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(properties);
    try {
      // Subscribe
      kafkaConsumer.subscribe(Arrays.asList(topic));

      // Start polling
      BenchmarkConsumer benchmarkConsumer =
          new KafkaBenchmarkConsumer(kafkaConsumer, consumerCallback);

      // Add to consumer list to close later
      consumers.add(benchmarkConsumer);
      return CompletableFuture.completedFuture(benchmarkConsumer);
    } catch (Throwable t) {
      kafkaConsumer.close();
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
    admin.close();
  }

  private String createEventHubConnectionString(
      String namespaceName, String domainName, String sasKeyName, String sasKeyValue) {
    return new ConnectionStringBuilder()
        .setEndpoint(namespaceName, StringUtils.stripStart(domainName, "."))
        .setSasKeyName(sasKeyName)
        .setSasKey(sasKeyValue)
        .toString();
  }
}

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

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import microsoft.azure.eventhub.perftesting.framework.driver.base.BenchmarkProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaBenchmarkProducer implements BenchmarkProducer {

  private final KafkaProducer<String, byte[]> producer;
  private final String topic;
  Logger log = LoggerFactory.getLogger(KafkaBenchmarkProducer.class);
  private boolean isProducerClosed = false;

  public KafkaBenchmarkProducer(KafkaProducer<String, byte[]> producer, String topic) {
    this.producer = producer;
    this.topic = topic;
  }

  @Override
  public CompletableFuture<Integer> sendAsync(Optional<String> key, byte[] payload) {
    ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key.orElse(null), payload);

    CompletableFuture<Integer> future = new CompletableFuture<>();

    producer.send(
        record,
        (metadata, exception) -> {
          if (exception != null) {
            future.completeExceptionally(exception);
          } else {
            future.complete(1);
          }
        });

    return future;
  }

  @Override
  public void close() throws Exception {
    try {
      log.info("Got command to close KafkaProducerClient");
      if (!isProducerClosed) {
        producer.close();
        isProducerClosed = true;
        log.info("Successfully closed Kafka Producer");
      }
    } catch (Exception e) {
      log.error(
          "Caught exception while trying to close KafkaProducer {} - {}",
          producer,
          Arrays.toString(e.getStackTrace()));
    }
  }
}

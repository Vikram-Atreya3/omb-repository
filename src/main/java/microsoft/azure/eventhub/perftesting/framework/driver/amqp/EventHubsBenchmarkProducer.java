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
package microsoft.azure.eventhub.perftesting.framework.driver.amqp;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import microsoft.azure.eventhub.perftesting.framework.driver.base.BenchmarkProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EventHubsBenchmarkProducer implements BenchmarkProducer {
  private static final Logger log = LoggerFactory.getLogger(EventHubsBenchmarkProducer.class);

  private final EventHubProducerAsyncClient producerClient;
  private final int batchCount;
  private final int batchSize;
  private EventDataBatch eventDataBatch;
  private boolean isProducerClosed = false;
  private CreateBatchOptions batchOptions;

  public EventHubsBenchmarkProducer(
      EventHubProducerAsyncClient producerClient, Properties producerProperties) {
    this.producerClient = producerClient;
    this.batchCount = Integer.parseInt(producerProperties.getProperty("batch.count"));
    this.batchSize = Integer.parseInt(producerProperties.getProperty("batch.size"));
    batchOptions = new CreateBatchOptions().setMaximumSizeInBytes(batchSize);
    eventDataBatch = producerClient.createBatch(batchOptions).block();
  }

  @Override
  public CompletableFuture<Integer> sendAsync(Optional<String> key, byte[] payload) {

    CompletableFuture<Integer> future = new CompletableFuture<>();
    if (isProducerClosed) {
      future.completeExceptionally(
          new RuntimeException("Producer Client is closed. Failing the send call"));
      return future;
    }

    EventData event = new EventData(payload);
    event.getProperties().putIfAbsent("producer_timestamp", System.currentTimeMillis());
    boolean addSuccessful = eventDataBatch.tryAdd(event);

    if (!addSuccessful) {
      final int messagesToBeSent = eventDataBatch.getCount();
      // EventDataBatch is full. Send the existing batch and then add the current data.
      producerClient
          .send(eventDataBatch)
          .subscribe(
              unused -> {}, future::completeExceptionally, () -> future.complete(messagesToBeSent));
      eventDataBatch = producerClient.createBatch(batchOptions).block();
      eventDataBatch.tryAdd(event);

    } else {
      if (eventDataBatch.getCount() >= batchCount) {
        producerClient
            .send(eventDataBatch)
            .subscribe(
                unused -> {}, future::completeExceptionally, () -> future.complete(batchCount));
        eventDataBatch = producerClient.createBatch(batchOptions).block();
      } else {
        future.complete(0);
      }
    }
    return future;
  }

  @Override
  public void close() throws Exception {
    log.info("Got command to close EventHubProducerClient");
    try {
      if (!isProducerClosed) {
        producerClient.close();
        isProducerClosed = true;
        log.info("Successfully closed EH Producer");
      }
    } catch (Exception e) {
      // Anti-pattern maybe but required for system stability for now.
      log.error(
          "Caught exception while trying to close EventHubProducer {} - {}",
          producerClient,
          Arrays.toString(e.getStackTrace()));
    }
  }
}

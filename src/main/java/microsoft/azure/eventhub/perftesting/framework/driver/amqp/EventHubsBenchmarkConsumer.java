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

import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.models.EventContext;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import microsoft.azure.eventhub.perftesting.framework.driver.base.BenchmarkConsumer;
import microsoft.azure.eventhub.perftesting.framework.driver.base.ConsumerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EventHubsBenchmarkConsumer implements BenchmarkConsumer {
  private static final Logger log = LoggerFactory.getLogger(EventHubsBenchmarkConsumer.class);

  private final EventProcessorClient eventProcessorClient;
  private final ExecutorService executor;
  private final Future<?> consumerTask;

  public EventHubsBenchmarkConsumer(EventProcessorClient eventProcessorClient) {
    this.eventProcessorClient = eventProcessorClient;
    this.executor = Executors.newSingleThreadExecutor();
    this.consumerTask = this.executor.submit(eventProcessorClient::start);
  }

  public static void processEvent(EventContext eventContext, ConsumerCallback consumerCallback) {
    consumerCallback.messageReceived(
        eventContext.getEventData().getBody(),
        TimeUnit.MILLISECONDS.toNanos(
            Long.parseLong(
                eventContext.getEventData().getProperties().get("producer_timestamp").toString())));
    if (eventContext.getEventData().getSequenceNumber() % 100 == 0) {
      eventContext
          .updateCheckpointAsync()
          .doOnError(throwable -> log.error("Got error while updating checkpoint.", throwable));
    }
  }

  @Override
  public void close() throws Exception {
    log.warn("Shutting down EventHubConsumer gracefully");
    executor.shutdown();
    consumerTask.get();
    eventProcessorClient.stop();
  }
}

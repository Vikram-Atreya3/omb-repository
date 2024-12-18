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

import java.io.IOException;
import java.util.List;
import microsoft.azure.eventhub.perftesting.framework.driver.DriverConfiguration;
import microsoft.azure.eventhub.perftesting.framework.worker.commands.*;

public interface Worker extends AutoCloseable {

  void initializeDriver(DriverConfiguration driverConfiguration) throws IOException;

  List<Topic> createTopics(TopicsInfo topicsInfo) throws IOException;

  // Let other workers know when a new topic(s) is created
  void notifyTopicCreation(List<Topic> topics) throws IOException;

  void createProducers(List<String> topics) throws IOException;

  void createConsumers(ConsumerAssignment consumerAssignment) throws IOException;

  void probeProducers() throws IOException;

  void startLoad(ProducerWorkAssignment producerWorkAssignment) throws IOException;

  void adjustPublishRate(double publishRate) throws IOException;

  void pauseConsumers() throws IOException;

  void resumeConsumers() throws IOException;

  void pauseProducers() throws IOException;

  void resumeProducers() throws IOException;

  void healthCheck() throws IOException;

  CountersStats getCountersStats() throws IOException;

  PeriodStats getPeriodStats() throws IOException;

  CumulativeLatencies getCumulativeLatencies() throws IOException;

  void resetStats() throws IOException;

  void stopAll() throws IOException;
}

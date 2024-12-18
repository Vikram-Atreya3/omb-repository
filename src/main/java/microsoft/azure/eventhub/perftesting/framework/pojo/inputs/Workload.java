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
package microsoft.azure.eventhub.perftesting.framework.pojo.inputs;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.Optional;
import lombok.Getter;
import lombok.ToString;
import microsoft.azure.eventhub.perftesting.framework.utils.distributor.KeyDistributorType;

@ToString
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class Workload implements Serializable {
  public String name;

  /** Number of topics to create in the test */
  public Integer topics;

  /** Number of partitions each topic will contain */
  public Integer partitionsPerTopic;

  /** Set this field if you want to re-use an existing topic */
  public String topicName;

  public KeyDistributorType keyDistributor = KeyDistributorType.NO_KEY;

  public Integer messageSize;

  public String payloadFile;

  public Integer subscriptionsPerTopic;

  public Integer producersPerTopic;

  public Integer consumerPerSubscription;

  public Integer producerRate;

  /**
   * If the consumer backlog is > 0, the generator will accumulate messages until the requested
   * amount of storage is retained and then it will start the consumers to drain it.
   *
   * <p>The testDurationMinutes will be overruled to allow the test to complete when the consumer
   * has drained all the backlog and it's on par with the producer
   */
  public Long consumerBacklogSizeGB = 0L;

  public Integer testDurationMinutes;

  /**
   * Consumer only tests. The warn-up phase will be run w/ the specified producer configuration
   * until the specified backlog is built. Following that, the producers are shut off and the
   * consumers are run until the backlog is drained
   */
  public Boolean consumerOnly = false;

  public String mgmtOperation;
  public Integer waitTimeBeforeMgmtOperation = 0;
  @Getter private final Integer warmupTrafficDurationInMinutes = 4;

  public void validate() throws IllegalArgumentException {
    if (consumerOnly && (subscriptionsPerTopic == 0 || consumerPerSubscription == 0)) {
      throw new IllegalArgumentException("Consumer only tests need subscriptions/consumers");
    }

    if (consumerOnly && Optional.ofNullable(consumerBacklogSizeGB).orElseGet(() -> 0L) <= 0) {
      throw new IllegalArgumentException("Consumer only tests need a backlog specification");
    }

    if (producerRate <= 0) {
      throw new IllegalArgumentException("Producer rate should be > 0");
    }
  }
}

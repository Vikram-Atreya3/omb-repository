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
package microsoft.azure.eventhub.perftesting.framework.driver.base;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface BenchmarkProducer extends AutoCloseable {

  /**
   * Publish a message and return a callback to track the completion of the operation.
   *
   * @param key the key associated with this message
   * @param payload the message payload
   * @return a future that will be triggered when the message is successfully published
   */
  CompletableFuture<Integer> sendAsync(Optional<String> key, byte[] payload);
}

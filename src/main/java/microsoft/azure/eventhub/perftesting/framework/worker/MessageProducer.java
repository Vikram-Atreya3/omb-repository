/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package microsoft.azure.eventhub.perftesting.framework.worker;

import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import microsoft.azure.eventhub.perftesting.framework.driver.base.BenchmarkProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducer {

  private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);
  private final WorkerStats stats;
  private RateLimiter rateLimiter;

  MessageProducer(RateLimiter rateLimiter, WorkerStats stats) {
    this.rateLimiter = rateLimiter;
    this.stats = stats;
  }

  public void sendMessage(BenchmarkProducer producer, String key, byte[] payload) {
    rateLimiter.acquire();
    final long sendTime = System.nanoTime();
    producer
        .sendAsync(Optional.ofNullable(key), payload)
        .thenAccept(
            messageSent -> {
              success(messageSent, payload.length, sendTime);
            })
        .exceptionally(this::failure);
  }

  private void success(long messageSent, long payloadLength, long sendTime) {
    long nowNs = System.nanoTime();
    stats.recordProducerSuccess(messageSent, payloadLength, sendTime, nowNs);
  }

  private Void failure(Throwable t) {
    stats.recordProducerFailure();
    log.warn("Write error on message", t);
    return null;
  }
}

package microsoft.azure.eventhub.perftesting.framework.worker;

import static microsoft.azure.eventhub.perftesting.framework.worker.WorkerHandler.*;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import microsoft.azure.eventhub.perftesting.framework.driver.DriverConfiguration;
import microsoft.azure.eventhub.perftesting.framework.worker.commands.*;
import org.apache.commons.lang.SerializationUtils;
import org.asynchttpclient.AsyncHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPWorkerClient implements Worker {
  private static final int REQUEST_TIMEOUT_MS = 300_000;
  private static final int READ_TIMEOUT_MS = 300_000;

  private static final byte[] EMPTY_BODY = new byte[0];
  private static final int HTTP_OK = 200;
  private static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static final Logger log = LoggerFactory.getLogger(HTTPWorkerClient.class);

  static {
    mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
  }

  private final AsyncHttpClient httpClient;
  private final String host;

  public HTTPWorkerClient(String host) {
    this.host = host;
    this.httpClient =
        asyncHttpClient(
            config().setRequestTimeout(REQUEST_TIMEOUT_MS).setReadTimeout(READ_TIMEOUT_MS));
  }

  @Override
  public void initializeDriver(DriverConfiguration driverConfiguration) throws IOException {
    byte[] confFileContent = SerializationUtils.serialize(driverConfiguration);
    sendPost(INITIALIZE_DRIVER, confFileContent);
  }

  @Override
  public List<Topic> createTopics(TopicsInfo topicsInfo) throws IOException {
    return post(
        CREATE_TOPICS, writer.writeValueAsBytes(topicsInfo), new TypeReference<List<Topic>>() {});
  }

  @Override
  public void notifyTopicCreation(List<Topic> topics) throws IOException {
    sendPost(NOTIFY_TOPIC_CREATION, writer.writeValueAsBytes(topics));
  }

  @Override
  public void createProducers(List<String> topics) throws IOException {
    sendPost(CREATE_PRODUCERS, writer.writeValueAsBytes(topics));
  }

  @Override
  public void createConsumers(ConsumerAssignment consumerAssignment) throws IOException {
    sendPost(CREATE_CONSUMERS, writer.writeValueAsBytes(consumerAssignment));
  }

  @Override
  public void probeProducers() throws IOException {
    sendPost(PROBE_PRODUCERS);
  }

  @Override
  public void startLoad(ProducerWorkAssignment producerWorkAssignment) throws IOException {
    log.info(
        "Setting worker assigned publish rate to {} msgs/sec", producerWorkAssignment.publishRate);
    sendPost(START_LOAD, writer.writeValueAsBytes(producerWorkAssignment));
  }

  @Override
  public void adjustPublishRate(double publishRate) throws IOException {
    log.info("Adjusting worker publish rate to {} msgs/sec", publishRate);
    sendPost(ADJUST_PUBLISH_RATE, writer.writeValueAsBytes(publishRate));
  }

  @Override
  public void pauseConsumers() throws IOException {
    sendPost(PAUSE_CONSUMERS);
  }

  @Override
  public void resumeConsumers() throws IOException {
    sendPost(RESUME_CONSUMERS);
  }

  @Override
  public void pauseProducers() throws IOException {
    sendPost(PAUSE_PRODUCERS);
  }

  @Override
  public void resumeProducers() throws IOException {
    sendPost(RESUME_CONSUMERS);
  }

  @Override
  public void healthCheck() throws IOException {
    get(HEALTH_CHECK, String.class);
  }

  @Override
  public CountersStats getCountersStats() throws IOException {
    return get(COUNTERS_STATS, CountersStats.class);
  }

  @Override
  public PeriodStats getPeriodStats() throws IOException {
    final PeriodStats periodStats = get(PERIOD_STATS, PeriodStats.class);
    return periodStats;
  }

  @Override
  public CumulativeLatencies getCumulativeLatencies() throws IOException {
    return get(CUMULATIVE_LATENCIES, CumulativeLatencies.class);
  }

  @Override
  public void resetStats() throws IOException {
    sendPost(RESET_STATS);
  }

  @Override
  public void stopAll() throws IOException {
    sendPost(STOP_ALL);
  }

  @Override
  public void close() throws Exception {
    httpClient.close();
  }

  private void sendPost(String path) {
    sendPost(path, EMPTY_BODY);
  }

  private void sendPost(String path, byte[] body) {
    httpClient
        .preparePost(host + path)
        .setBody(body)
        .execute()
        .toCompletableFuture()
        .thenApply(
            response -> {
              if (response.getStatusCode() != HTTP_OK) {
                log.error(
                    "Failed to do HTTP post request to {}{} -- code: {}",
                    host,
                    path,
                    response.getStatusCode());
              }
              Preconditions.checkArgument(response.getStatusCode() == HTTP_OK);
              return (Void) null;
            })
        .join();
  }

  private <T> T get(String path, Class<T> clazz) {
    return httpClient
        .prepareGet(host + path)
        .execute()
        .toCompletableFuture()
        .thenApply(
            response -> {
              try {
                if (response.getStatusCode() != HTTP_OK) {
                  log.error(
                      "Failed to do HTTP get request to {}{} -- code: {}",
                      host,
                      path,
                      response.getStatusCode());
                }
                Preconditions.checkArgument(response.getStatusCode() == HTTP_OK);
                return mapper.readValue(response.getResponseBody(), clazz);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .join();
  }

  private <T> T post(String path, byte[] body, TypeReference<T> type) {
    return httpClient
        .preparePost(host + path)
        .setBody(body)
        .execute()
        .toCompletableFuture()
        .thenApply(
            response -> {
              try {
                if (response.getStatusCode() != 200) {
                  log.error(
                      "Failed to do HTTP post request to {}{} -- code: {}",
                      host,
                      path,
                      response.getStatusCode());
                }
                Preconditions.checkArgument(response.getStatusCode() == 200);
                return mapper.readValue(response.getResponseBody(), type);
              } catch (IOException e) {
                log.error("Found Error while calling {} for host {}", path, host);
                throw new RuntimeException(e);
              }
            })
        .join();
  }

  @Override
  public String toString() {
    return "HTTPWorkerClient{" + "host='" + host + '\'' + '}';
  }
}

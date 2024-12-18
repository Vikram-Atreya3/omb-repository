package microsoft.azure.eventhub.perftesting.framework.driver;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import microsoft.azure.eventhub.perftesting.framework.appconfigadapter.ConfigProvider;
import microsoft.azure.eventhub.perftesting.framework.appconfigadapter.ConfigurationKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EventHubHTTPCrud {

  private static final Logger log = LoggerFactory.getLogger(EventHubHTTPCrud.class);
  private final NamespaceMetadata metadata;
  private final ConfigProvider configProvider;
  private final HttpClient httpClient;

  public EventHubHTTPCrud(NamespaceMetadata metadata, ConfigProvider configProvider) {
    this.metadata = metadata;
    this.configProvider = configProvider;
    this.httpClient = HttpClient.newHttpClient();
  }

  private String getSASToken(String resourceUri, String keyName, String key) {
    long epoch = System.currentTimeMillis() / 1000L;
    int week = 60 * 60 * 24 * 7;
    String expiry = Long.toString(epoch + week);

    String sasToken = null;
    try {
      String stringToSign = URLEncoder.encode(resourceUri, "UTF-8") + "\n" + expiry;
      String signature = getHMAC256(key, stringToSign);
      sasToken =
          "SharedAccessSignature sr="
              + URLEncoder.encode(resourceUri, "UTF-8")
              + "&sig="
              + URLEncoder.encode(signature, "UTF-8")
              + "&se="
              + expiry
              + "&skn="
              + keyName;
    } catch (UnsupportedEncodingException e) {
      log.error("Got error while encoding SASKey - {}", e.getMessage());
    }

    return sasToken;
  }

  private String getHMAC256(String key, String input) {
    Mac sha256_HMAC = null;
    String hash = null;
    try {
      sha256_HMAC = Mac.getInstance("HmacSHA256");
      SecretKeySpec secret_key = new SecretKeySpec(key.getBytes(), "HmacSHA256");
      sha256_HMAC.init(secret_key);
      Base64.Encoder encoder = Base64.getEncoder();

      hash = new String(encoder.encode(sha256_HMAC.doFinal(input.getBytes("UTF-8"))));

    } catch (Exception e) {
      log.error("Caught error while encrypting sasKey {}", e.getMessage());
    }

    return hash;
  }

  protected void createTopic(String topic, int partitions) {
    String sasToken =
        getSASToken(
            this.metadata.namespaceName
                + configProvider.getConfigurationValue(ConfigurationKey.FQDNSuffix),
            metadata.sasKeyName,
            metadata.sasKeyValue);
    String url =
        String.format(
            "https://%s/%s?api-version=2023-07-01",
            this.metadata.namespaceName
                + configProvider.getConfigurationValue(ConfigurationKey.FQDNSuffix),
            topic);
    String body =
        String.format(
            "<entry xmlns=\"http://www.w3.org/2005/Atom\"><content type=\"application/xml\"><EventHubDescription xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"http://schemas.microsoft.com/netservices/2010/10/servicebus/connect\"><SkippedUpdate>false</SkippedUpdate><PartitionCount>%s</PartitionCount></EventHubDescription></content></entry>",
            partitions);
    HttpRequest request =
        HttpRequest.newBuilder()
            .PUT(HttpRequest.BodyPublishers.ofString(body))
            .uri(URI.create(url))
            .setHeader("Authorization", sasToken)
            .setHeader("Content-Type", "application/atom+xml")
            .build();
    try {
      var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      log.info(
          "CreateTopic HTTP ResponseCode {} an Response {}",
          response.statusCode(),
          response.body());
    } catch (Exception e) {
      log.error(
          "Caught error {} while creating topic {} inside namespace {}",
          e.getMessage(),
          topic,
          metadata.namespaceName);
      throw new RuntimeException(e);
    }
  }

  protected void createConsumerGroupIfNotPresent(String topicName, String subscriptionName) {
    String sasToken =
        getSASToken(
            this.metadata.namespaceName
                + configProvider.getConfigurationValue(ConfigurationKey.FQDNSuffix),
            metadata.sasKeyName,
            metadata.sasKeyValue);
    String url =
        String.format(
            "https://%s/%s/ConsumerGroups/%s?api-version=2023-07-01",
            this.metadata.namespaceName
                + configProvider.getConfigurationValue(ConfigurationKey.FQDNSuffix),
            topicName,
            subscriptionName);
    String body =
        String.format(
            "<entry xmlns=\"http://www.w3.org/2005/Atom\"><content type=\"application/xml\"><ConsumerGroupDescription xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"http://schemas.microsoft.com/netservices/2010/10/servicebus/connect\"><SkippedUpdate>false</SkippedUpdate></ConsumerGroupDescription></content></entry>");
    System.out.println(url);
    System.out.println(body);
    HttpRequest request =
        HttpRequest.newBuilder()
            .PUT(HttpRequest.BodyPublishers.ofString(body))
            .uri(URI.create(url))
            .setHeader("Authorization", sasToken)
            .setHeader("Content-Type", "application/atom+xml")
            .build();
    try {
      var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      log.info(
          "CreateConsumerGroup HTTP ResponseCode {} an Response {}",
          response.statusCode(),
          response.body());
    } catch (Exception e) {
      log.error(
          "Caught error {} while creating consumerGroup {} inside topic {} and namespace {}",
          e.getMessage(),
          subscriptionName,
          topicName,
          metadata.namespaceName);
      throw new RuntimeException(e);
    }
  }
}

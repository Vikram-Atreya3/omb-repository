package microsoft.azure.eventhub.perftesting.framework.driver;

import static microsoft.azure.eventhub.perftesting.framework.appconfigadapter.EnvironmentName.Production;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.resourcemanager.eventhubs.EventHubsManager;
import com.azure.resourcemanager.eventhubs.models.AccessRights;
import com.azure.resourcemanager.eventhubs.models.EventHub;
import com.azure.resourcemanager.eventhubs.models.EventHubNamespaceAuthorizationRule;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import microsoft.azure.eventhub.perftesting.framework.appconfigadapter.ConfigProvider;
import microsoft.azure.eventhub.perftesting.framework.appconfigadapter.ConfigurationKey;
import microsoft.azure.eventhub.perftesting.framework.credetentialadapter.CredentialProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHubAdministrator {
  private static final Logger log = LoggerFactory.getLogger(EventHubAdministrator.class);
  static NamespaceMetadata metadata;
  static ConfigProvider configProvider;
  static CredentialProvider credentialProvider;
  static AzureEnvironment azureEnvironment;
  private static volatile EventHubAdministrator ehAdminInstance;
  private final TokenCredential sharedCSC;
  private final AzureProfile sharedAzureProfile;
  private final EventHubsManager manager;
  private final EventHubHTTPCrud eventHubHTTPCrud;
  private final HttpClient httpClient;

  public static EventHubAdministrator getInstance(NamespaceMetadata namespaceMetadata) {
    if (ehAdminInstance == null) {
      synchronized (EventHubAdministrator.class) {
        if (ehAdminInstance == null) {
          ehAdminInstance = new EventHubAdministrator(namespaceMetadata);
        }
      }
    }
    return ehAdminInstance;
  }

  private EventHubAdministrator(NamespaceMetadata namespaceMetadata) {
    credentialProvider = CredentialProvider.getInstance();
    metadata = namespaceMetadata;
    configProvider = ConfigProvider.getInstance();

    if (configProvider.getEnvironmentStage().equalsIgnoreCase(Production.name())) {
      azureEnvironment = AzureEnvironment.AZURE;
    } else {
      // Configure non Azure Prod Endpoint in Azure Config.
      azureEnvironment =
          new AzureEnvironment(
              new HashMap<String, String>() {
                {
                  put("managementEndpointUrl", "https://management.core.windows.net/");
                  put(
                      "resourceManagerEndpointUrl",
                      configProvider.getConfigurationValue(ConfigurationKey.ResourceManagementURL));
                  put(
                      "activeDirectoryEndpointUrl",
                      configProvider.getConfigurationValue(ConfigurationKey.AuthorityHost));
                  put("microsoftGraphResourceId", "https://graph.windows.net/");
                }
              });
    }

    sharedCSC = createClientSecretCredential();
    sharedAzureProfile = createAzureProfile(namespaceMetadata);
    this.httpClient = java.net.http.HttpClient.newHttpClient();
    manager = EventHubsManager.configure().authenticate(sharedCSC, sharedAzureProfile);
    eventHubHTTPCrud = new EventHubHTTPCrud(metadata, configProvider);
  }

  private static AzureProfile createAzureProfile(NamespaceMetadata metadata) {
    return new AzureProfile(
        configProvider.getConfigurationValue(ConfigurationKey.ApplicationTenantID),
        metadata.subscriptionId,
        azureEnvironment);
  }

  private static TokenCredential createClientSecretCredential() {
    return new ClientSecretCredentialBuilder()
        .clientSecret(
            credentialProvider.getCredential(
                configProvider.getEnvironmentStage() + "AAD" + "ClientSecret"))
        .clientId(
            credentialProvider.getCredential(
                configProvider.getEnvironmentStage() + "AAD" + "ClientId"))
        .tenantId(configProvider.getConfigurationValue(ConfigurationKey.ApplicationTenantID))
        .authorityHost(configProvider.getConfigurationValue(ConfigurationKey.AuthorityHost))
        .build();
  }

  public void createTopic(String topic, int partitions) {
    try {
      final EventHub eventHub =
          manager
              .namespaces()
              .eventHubs()
              .getByName(metadata.resourceGroup, metadata.namespaceName, topic);
      log.info(
          "Reusing the existing topic as it exists - "
              + eventHub.name()
              + " with partition counts "
              + (long) eventHub.partitionIds().size());
    } catch (Exception e) {
      log.info(" Creating new topic with Topic Name: " + topic);
      try {
        manager
            .namespaces()
            .eventHubs()
            .define(topic)
            .withExistingNamespace(metadata.resourceGroup, metadata.namespaceName)
            .withPartitionCount(partitions)
            .create();
      } catch (Exception ex) {
        log.error(
            "ARM call failed while creating topic {} in namespace {}. Error Reason - {}",
            topic,
            metadata.namespaceName,
            ex.getMessage());
        eventHubHTTPCrud.createTopic(topic, partitions);
      }
    }
  }

  public EventHubNamespaceAuthorizationRule getAuthorizationRule() {
    final PagedIterable<EventHubNamespaceAuthorizationRule> eventHubNamespaceAuthorizationRules =
        manager
            .namespaceAuthorizationRules()
            .listByNamespace(metadata.resourceGroup, metadata.namespaceName);
    return eventHubNamespaceAuthorizationRules.stream()
        .filter(authRule -> authRule.rights().contains(AccessRights.MANAGE))
        .findFirst()
        .orElseThrow(RuntimeException::new);
  }

  public List<EventHub> listTopicForNamespace() {
    try {
      return manager
          .namespaces()
          .eventHubs()
          .listByNamespace(metadata.resourceGroup, metadata.namespaceName)
          .stream()
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("ARM Error Caught - {}", e.getMessage());
      log.error(
          "Could not fetch eventHub for namespace {}. Returning Empty List",
          metadata.namespaceName);
      return new ArrayList<>();
    }
  }

  public void createConsumerGroupIfNotPresent(String topicName, String subscriptionName) {
    try {
      manager
          .consumerGroups()
          .define(subscriptionName)
          .withExistingEventHub(metadata.resourceGroup, metadata.namespaceName, topicName)
          .create();
    } catch (Exception e) {
      log.error(
          "ARM Call Failed while creating consumerGroup {} for topic {} in namespace{}",
          subscriptionName,
          topicName,
          metadata.namespaceName);
      log.error("ARM Error Caught - {}", e.getMessage());
      eventHubHTTPCrud.createConsumerGroupIfNotPresent(topicName, subscriptionName);
    }
  }

  public void deleteTopic(String topicName) {
    manager
        .namespaces()
        .eventHubs()
        .deleteByName(metadata.resourceGroup, metadata.namespaceName, topicName);
  }

  public void runArmRequest(String urlSuffix, String body, String requestType)
      throws IOException, InterruptedException {
    AccessToken token =
        sharedCSC
            .getToken(new TokenRequestContext().addScopes("https://management.azure.com/.default"))
            .block();
    if (token.toString().isBlank() || token.isExpired()) {
      log.error("Error in acquiring Bearer Token");
    }
    String bearerToken = "Bearer " + token.getToken();
    URI uri =
        URI.create(
            configProvider.getConfigurationValue(ConfigurationKey.ResourceManagementURL)
                + "/"
                + urlSuffix);
    HttpRequest armRequest =
        HttpRequest.newBuilder()
            .uri(uri)
            .method(requestType, HttpRequest.BodyPublishers.ofString(body))
            .setHeader("Authorization", bearerToken)
            .setHeader("Content-Type", "application/xml")
            .build();

    var armResponse =
        httpClient.send(armRequest, java.net.http.HttpResponse.BodyHandlers.ofString());
    log.info("Arm Response status code:" + armResponse.statusCode());
    if(armResponse.statusCode() == 200) {
      log.error(armResponse.body());
    }

  }
}

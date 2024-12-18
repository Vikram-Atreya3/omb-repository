package microsoft.azure.eventhub.perftesting.framework.driver.mgmtOperations.overrides;

import java.io.IOException;
import microsoft.azure.eventhub.perftesting.framework.driver.EventHubAdministrator;
import microsoft.azure.eventhub.perftesting.framework.driver.NamespaceMetadata;
import microsoft.azure.eventhub.perftesting.framework.driver.mgmtOperations.MgmtOperationThread;
import microsoft.azure.eventhub.perftesting.framework.pojo.inputs.Workload;

public class GeoDRFailover extends MgmtOperationThread {

  public GeoDRFailover(Workload workload, NamespaceMetadata namespaceMetadata) {
    this.workload = workload;
    this.namespaceMetadata = namespaceMetadata;
    this.ehAdmin = EventHubAdministrator.getInstance(namespaceMetadata);
  }

  @Override
  public void run() {
    String urlSuffix =
        "subscriptions/"
            + namespaceMetadata.subscriptionId
            + "/resourceGroups/"
            + namespaceMetadata.resourceGroup
            + "/providers/Microsoft.EventHub/namespaces/"
            + namespaceMetadata.namespaceName
            + "/failover?api-version=2023-01-01-preview";
    String body =
        "{\n"
            + "    \"properties\": {\n"
            + "        \"PrimaryLocation\" : \""
            + "westus"
            + "\",\n"
            + "        \"Force\" : true\n"
            + "    }\n"
            + "}";
    try {
      ehAdmin.runArmRequest(urlSuffix, body, "POST");
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}

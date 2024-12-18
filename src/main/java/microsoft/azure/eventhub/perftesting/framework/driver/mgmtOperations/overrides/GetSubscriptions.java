package microsoft.azure.eventhub.perftesting.framework.driver.mgmtOperations.overrides;

import java.io.IOException;
import microsoft.azure.eventhub.perftesting.framework.driver.EventHubAdministrator;
import microsoft.azure.eventhub.perftesting.framework.driver.NamespaceMetadata;
import microsoft.azure.eventhub.perftesting.framework.driver.mgmtOperations.MgmtOperationThread;
import microsoft.azure.eventhub.perftesting.framework.pojo.inputs.Workload;

public class GetSubscriptions extends MgmtOperationThread {

  public GetSubscriptions(Workload workload, NamespaceMetadata namespaceMetadata) {
    this.workload = workload;
    this.namespaceMetadata = namespaceMetadata;
    this.ehAdmin = EventHubAdministrator.getInstance(namespaceMetadata);
  }

  @Override
  public void run() {
    // command is run after wait time specified in workload
    String urlSuffix =
        "subscriptions/" + namespaceMetadata.subscriptionId + "?api-version=2016-09-01";
    String body = "";
    try {
      ehAdmin.runArmRequest(urlSuffix, body, "GET");
      // Can add more wait time her and run subsequent requests
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}

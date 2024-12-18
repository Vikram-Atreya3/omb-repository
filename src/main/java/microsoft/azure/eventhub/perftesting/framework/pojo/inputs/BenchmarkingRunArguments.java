package microsoft.azure.eventhub.perftesting.framework.pojo.inputs;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.*;
import microsoft.azure.eventhub.perftesting.framework.driver.DriverConfiguration;
import microsoft.azure.eventhub.perftesting.framework.driver.NamespaceMetadata;

@Builder
@Getter
@ToString
public class BenchmarkingRunArguments {
  // These parameters need to be set at the time of creation of the arguments
  @NonNull String testName;
  String testSuiteName;
  @NonNull DriverConfiguration driver;
  @NonNull Workload workload;
  @NonNull Payload messagePayload;

  // These can be set dynamically afterwards or by the benchmark framework.
  @Setter NamespaceMetadata namespaceMetadata;
  @Setter String runID;

  WorkerAllocations workerAllocation;

  List<String> tags;
  String runUserId;

  public void validate() {
    // Other necessary parameters are already mentioned as non-null
    workerAllocation.validateAndSetDefaults();
    Preconditions.checkNotNull(namespaceMetadata);
    Preconditions.checkNotNull(runID);
    workload.validate();
    messagePayload.validate();
  }
}

package microsoft.azure.eventhub.perftesting.framework.driver;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class NamespaceMetadata implements Serializable {
  public String namespaceName;
  public String subscriptionId;
  public String resourceGroup;
  public String sasKeyName;
  @ToString.Exclude public String sasKeyValue;
  public String region;
}

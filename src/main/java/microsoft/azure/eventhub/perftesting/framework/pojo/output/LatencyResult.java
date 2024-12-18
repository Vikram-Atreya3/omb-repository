package microsoft.azure.eventhub.perftesting.framework.pojo.output;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Data
public class LatencyResult {
  public String uuid;
  public String timestamp;
  @Builder.Default public LatencyMetric latencyMetric = new LatencyMetric();
}

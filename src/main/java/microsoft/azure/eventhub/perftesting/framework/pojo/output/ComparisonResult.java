package microsoft.azure.eventhub.perftesting.framework.pojo.output;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class ComparisonResult {
  String metricName;
  double currentValue;
  double expectedValue;
}

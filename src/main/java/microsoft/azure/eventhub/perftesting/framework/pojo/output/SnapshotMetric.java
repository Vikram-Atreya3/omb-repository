package microsoft.azure.eventhub.perftesting.framework.pojo.output;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Data
public class SnapshotMetric extends OMBMetrics {

  public long timeSinceTestStartInSeconds;
  public long backlog;
}

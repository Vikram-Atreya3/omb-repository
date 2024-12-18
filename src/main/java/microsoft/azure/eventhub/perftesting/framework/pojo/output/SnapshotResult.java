package microsoft.azure.eventhub.perftesting.framework.pojo.output;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Data
public class SnapshotResult extends LatencyResult {

  public long timeSinceTestStartInSeconds;

  public Double publishRate;
  public Double consumeRate;
  public Double publishErrorRate;
  public long backlog;
}

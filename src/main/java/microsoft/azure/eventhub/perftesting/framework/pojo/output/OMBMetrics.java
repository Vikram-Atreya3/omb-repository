package microsoft.azure.eventhub.perftesting.framework.pojo.output;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Data
public class OMBMetrics {
  public String uuid;
  public String timestamp;
  @Builder.Default public LatencyMetric latencyMetric = new LatencyMetric();
  @Builder.Default public Double publishRate = 0.0;
  @Builder.Default public Double consumeRate = 0.0;
  @Builder.Default public Double publishErrorRate = 0.0;

  public void calculateMovingAverage(OMBMetrics snapshot, int datapointCounts) {
    this.publishRate =
        ((publishRate * datapointCounts) + snapshot.publishRate) / (datapointCounts + 1);
    this.consumeRate =
        ((consumeRate * datapointCounts) + snapshot.consumeRate) / (datapointCounts + 1);
    this.publishErrorRate =
        ((publishErrorRate * datapointCounts) + snapshot.publishErrorRate) / (datapointCounts + 1);
  }
}

package microsoft.azure.eventhub.perftesting.framework.pojo.output;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.HdrHistogram.Histogram;
import org.apache.commons.math3.util.Precision;

@SuperBuilder
@NoArgsConstructor
@Data
public class LatencyMetric implements Serializable {

  public Double publishLatencyAvg;
  public Double publishLatency95pct;
  public Double publishLatency99pct;
  public Double publishLatency999pct;
  public Double publishLatency9999pct;
  public Double publishLatencyMax;

  public Double endToEndLatencyAvg;
  public Double endToEndLatency95pct;
  public Double endToEndLatency99pct;
  public Double endToEndLatency999pct;
  public Double endToEndLatency9999pct;
  public Double endToEndLatencyMax;

  private static double microsToMillis(double microTime) {
    return Precision.round(microTime / 1000, 2);
  }

  public void populatePublishLatency(Histogram publishLatency) {
    populateLatency(publishLatency, "publishLatency");
  }

  public void populateE2ELatency(Histogram endToEndLatency) {
    populateLatency(endToEndLatency, "endToEndLatency");
  }

  /**
   * Comparing two Latency Metric incorporating the acceptability of error denoted by @param
   * errorThreshold The first object should be the one compared to and the other should be the one
   * compared against.
   *
   * @return List which contains details about all the params that failed the comparison.
   */
  public List<ComparisonResult> compareAndEvaluateDiff(LatencyMetric other, double errorThreshold) {
    List<ComparisonResult> result = new ArrayList<>();

    for (Field field : this.getClass().getDeclaredFields()) {
      if (field.getType().equals(Double.class)) {
        try {
          field.setAccessible(true);
          Double currentValue = (Double) field.get(this);
          Double expectedValue = (Double) field.get(other);
          compareAndAppendReason(
              result, field.getName(), currentValue, expectedValue, errorThreshold);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Error accessing field: " + field.getName(), e);
        }
      }
    }

    return result;
  }

  @SuppressWarnings("checkstyle:Indentation")
  private void populateLatency(Histogram latency, String prefix) {
    try {
      for (Field field : this.getClass().getDeclaredFields()) {
        if (field.getType().equals(Double.class) && field.getName().startsWith(prefix)) {
          field.setAccessible(true);
          String suffix = field.getName().substring(prefix.length());
          double value =
              switch (suffix) {
                case "Avg" -> microsToMillis(latency.getMean());
                case "95pct" -> microsToMillis(latency.getValueAtPercentile(95));
                case "99pct" -> microsToMillis(latency.getValueAtPercentile(99));
                case "999pct" -> microsToMillis(latency.getValueAtPercentile(99.9));
                case "9999pct" -> microsToMillis(latency.getValueAtPercentile(99.99));
                case "Max" -> microsToMillis(latency.getMaxValue());
                default -> 0;
              };
          field.set(this, value);
        }
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Error accessing field during latency population", e);
    }
  }

  private void compareAndAppendReason(
      List<ComparisonResult> result,
      String metricName,
      Double currentValue,
      Double expectedValue,
      Double errorThreshold) {
    if (expectedValue != null
        && currentValue != null
        && currentValue > (1 + errorThreshold / 100) * expectedValue) {
      result.add(
          ComparisonResult.builder()
              .metricName(metricName)
              .currentValue(currentValue)
              .expectedValue(expectedValue)
              .build());
    }
  }
}

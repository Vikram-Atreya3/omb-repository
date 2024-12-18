package microsoft.azure.eventhub.perftesting.framework.pojo.output;

import lombok.Data;

@Data
public class TestDetails {
  public String runID;
  public String testName;
  public String testSuiteName;

  public String testStartTime;
  public long testRunDurationInMinutes;

  public String product;
  public String sku;
  public String protocol;
  public Metadata metadata;
}

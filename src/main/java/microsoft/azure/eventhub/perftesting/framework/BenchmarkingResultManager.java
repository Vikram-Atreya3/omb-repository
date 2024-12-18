package microsoft.azure.eventhub.perftesting.framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import microsoft.azure.eventhub.perftesting.framework.appconfigadapter.ConfigProvider;
import microsoft.azure.eventhub.perftesting.framework.appconfigadapter.ConfigurationKey;
import microsoft.azure.eventhub.perftesting.framework.kustoadapter.KustoAdapter;
import microsoft.azure.eventhub.perftesting.framework.pojo.output.TestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkingResultManager {
  private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static final Logger log = LoggerFactory.getLogger(BenchmarkingResultManager.class);
  private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
  static KustoAdapter adapter;
  static ConfigProvider provider;

  static {
    try {
      // Ensure that you have set EnvironmentVariable AppConfigConnectionString before calling this
      provider = ConfigProvider.getInstance();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

  public static void persistTestResults(
      TestResult result, String outputFileName, boolean persistResultToKusto) {
    try {
      String fileNamePrefix =
          outputFileName != null && outputFileName.length() > 0
              ? outputFileName
              : String.format(
                  "%s-%s-%s-%s",
                  result.testDetails.product,
                  result.testDetails.protocol,
                  result.testDetails.runID,
                  dateFormat.format(new Date()));

      WriteTestResults(fileNamePrefix, result);
      if (persistResultToKusto) {
        if (adapter == null) {
          adapter =
              new KustoAdapter(
                  provider.getConfigurationValue(ConfigurationKey.KustoEndpoint),
                  provider.getConfigurationValue(ConfigurationKey.KustoDatabaseName));
        }
        adapter.uploadDataToKustoCluster(fileNamePrefix);
      }
    } catch (Exception e) {
      log.error("Found error while persisting test results", e);
    }
  }

  private static void WriteTestResults(String fileNamePrefix, TestResult result)
      throws IOException {
    writer.writeValue(new File(fileNamePrefix + "-details.json"), result.testDetails);
    writer.writeValue(new File(fileNamePrefix + "-snapshot.json"), result.snapshotMetrics);
    writer.writeValue(new File(fileNamePrefix + "-aggregate.json"), result.aggregateResult);
  }
}

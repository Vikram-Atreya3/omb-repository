/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package microsoft.azure.eventhub.perftesting.framework;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import com.nimbusds.oauth2.sdk.util.CollectionUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import microsoft.azure.eventhub.perftesting.framework.appconfigadapter.ConfigProvider;
import microsoft.azure.eventhub.perftesting.framework.driver.DriverConfiguration;
import microsoft.azure.eventhub.perftesting.framework.driver.NamespaceMetadata;
import microsoft.azure.eventhub.perftesting.framework.pojo.inputs.BenchmarkingRunArguments;
import microsoft.azure.eventhub.perftesting.framework.pojo.inputs.Payload;
import microsoft.azure.eventhub.perftesting.framework.pojo.inputs.WorkerAllocations;
import microsoft.azure.eventhub.perftesting.framework.pojo.inputs.Workload;
import microsoft.azure.eventhub.perftesting.framework.pojo.output.TestResult;
import microsoft.azure.eventhub.perftesting.framework.utils.payload.FilePayloadReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Benchmark {

  private static final ObjectMapper mapper =
      new ObjectMapper(new YAMLFactory())
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static final Logger log = LoggerFactory.getLogger(Benchmark.class);
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

  public static void main(String[] args) throws Exception {
    final Arguments arguments = new Arguments();
    JCommander jc = new JCommander(arguments);
    jc.setProgramName("messaging-benchmark");

    try {
      jc.parse(args);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      jc.usage();
      System.exit(-1);
    }

    if (arguments.help) {
      jc.usage();
      System.exit(-1);
    }

    final List<BenchmarkingRunArguments> benchmarkingRunArguments =
        convertCommandLineArgumentsToBenchmarkingRunArguments(arguments);
    benchmarkingRunArguments.forEach(
        individualRun -> {
          try {
            TestResult testResult =
                BenchmarkingRunOrchestrator.executeBenchmarkingRun(individualRun);
            BenchmarkingResultManager.persistTestResults(
                testResult, arguments.output, arguments.visualizeUsingKusto);
          } catch (Exception e) {
            log.error(e.toString());
          } finally {
            System.exit(0);
          }
        });
  }

  private static List<BenchmarkingRunArguments>
      convertCommandLineArgumentsToBenchmarkingRunArguments(Arguments commandLineArguments)
          throws IOException {
    log.info(
        "Starting benchmark run with config: {}", writer.writeValueAsString(commandLineArguments));
    final List<String> benchmarkingWorkers = returnWorkersIfPresent(commandLineArguments);
    final List<Workload> workloads = getWorkloadsFromArguments(commandLineArguments);
    List<BenchmarkingRunArguments> result = new ArrayList<>();
    workloads.forEach(
        workload ->
            commandLineArguments.drivers.forEach(
                driverConfig -> {
                  try {
                    final DriverConfiguration driverConfiguration =
                        getDriverConfiguration(driverConfig);
                    final NamespaceMetadata namespaceMetadata =
                        getNamespaceMetadata(commandLineArguments, driverConfiguration.identifier);
                    final String testName =
                        commandLineArguments.output != null
                            ? commandLineArguments.output.split("-")[0]
                            : workload.name;
                    final Payload messagePayload =
                        new Payload(
                            workload.messageSize, workload.payloadFile, new FilePayloadReader());
                    result.add(
                        BenchmarkingRunArguments.builder()
                            .testName(testName)
                            .runID(UUID.randomUUID().toString())
                            .driver(driverConfiguration)
                            .workload(workload)
                            .namespaceMetadata(namespaceMetadata)
                            .messagePayload(messagePayload)
                            .workerAllocation(
                                WorkerAllocations.builder()
                                    .isRemoteWorkerRequired(
                                        CollectionUtils.isNotEmpty(benchmarkingWorkers))
                                    .totalWorkerNodes(benchmarkingWorkers)
                                    .producerWorkerNodeCount(commandLineArguments.producerWorkers)
                                    .build())
                            .tags(commandLineArguments.tags)
                            .build());
                  } catch (Exception e) {
                    log.error(
                        "Caught error {} while translating workload {} and driver {}",
                        e.getMessage(),
                        workload.name,
                        driverConfig);
                  }
                }));
    return result;
  }

  private static NamespaceMetadata getNamespaceMetadata(
      Arguments commandLineArguments, String testIdentifier) throws JsonProcessingException {
    String metadataString =
        commandLineArguments.namespaceMetadata != null
            ? commandLineArguments.namespaceMetadata
            : provider.getNamespaceMetaData(testIdentifier);

    if (metadataString != null) {
      return new ObjectMapper().readValue(metadataString, NamespaceMetadata.class);
    } else {
      throw new RuntimeException("Could not find Namespace Information. Breaking the test");
    }
  }

  private static List<String> returnWorkersIfPresent(Arguments arguments) throws IOException {
    if (arguments.workers != null && arguments.workersFile != null) {
      System.err.println("Only one between --workers and --workers-file can be specified");
      throw new RuntimeException("Conflict between worker roles");
    }
    List<String> workerList = new ArrayList<>();

    if (arguments.workers == null && arguments.workersFile == null) {
      File defaultFile = new File("workers.yaml");
      if (defaultFile.exists()) {
        log.info("Using default worker file workers.yaml");
        arguments.workersFile = defaultFile;
      }
    }

    if (arguments.workersFile != null) {
      log.info("Reading workers list from {}", arguments.workersFile);
      workerList = mapper.readValue(arguments.workersFile, Workers.class).workers;
    } else if (arguments.workers != null) {
      workerList = arguments.workers;
    }

    return workerList;
  }

  private static DriverConfiguration getDriverConfiguration(String driverConfig)
      throws IOException {
    return mapper.readValue(Resources.getResource(driverConfig), DriverConfiguration.class);
  }

  private static List<Workload> getWorkloadsFromArguments(Arguments arguments) throws IOException {
    List<Workload> workloadList = new ArrayList<>();
    for (String path : arguments.workloads) {
      try {
        final Workload workload = mapper.readValue(Resources.getResource(path), Workload.class);
        workloadList.add(workload);
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
        throw e;
      }
    }
    return workloadList;
  }
}

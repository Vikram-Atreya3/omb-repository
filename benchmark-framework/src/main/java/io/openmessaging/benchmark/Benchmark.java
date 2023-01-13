/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import io.openmessaging.benchmark.appconfig.adapter.ConfigProvider;
import io.openmessaging.benchmark.appconfig.adapter.ConfigurationKey;
import io.openmessaging.benchmark.appconfig.adapter.EnvironmentName;
import io.openmessaging.benchmark.appconfig.adapter.NamespaceMetadata;
import io.openmessaging.benchmark.kusto.adapter.KustoAdapter;
import io.openmessaging.benchmark.output.Metadata;
import io.openmessaging.benchmark.output.TestResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.openmessaging.benchmark.worker.DistributedWorkersEnsemble;
import io.openmessaging.benchmark.worker.LocalWorker;
import io.openmessaging.benchmark.worker.Worker;

public class Benchmark {

    static class Arguments {

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "-d",
                "--drivers" }, description = "Drivers list. eg.: pulsar/pulsar.yaml,kafka/kafka.yaml", required = true)
        public List<String> drivers;

        @Parameter(names = { "-w",
                "--workers" }, description = "List of worker nodes. eg: http://1.2.3.4:8080,http://4.5.6.7:8080")
        public List<String> workers;

        @Parameter(names = { "-wf",
                "--workers-file" }, description = "Path to a YAML file containing the list of workers addresses")
        public File workersFile;

        @Parameter(description = "Workloads", required = true)
        public List<String> workloads;

        @Parameter(names = { "-o", "--output" }, description = "Output", required = false)
        public String output;
    }

    static ConfigProvider provider;
    static KustoAdapter adapter;

    static {
        try {
            //Ensure that you have set EnvironmentVariable AppConfigConnectionString before calling this
            provider = ConfigProvider.getInstance(EnvironmentName.Production.toString());
            adapter = new KustoAdapter(provider.getConfigurationValue(ConfigurationKey.KustoEndpoint),
                    provider.getConfigurationValue(ConfigurationKey.KustoDatabaseName));

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

        if (arguments.workers != null && arguments.workersFile != null) {
            System.err.println("Only one between --workers and --workers-file can be specified");
            System.exit(-1);
        }

        if (arguments.workers == null && arguments.workersFile == null) {
            File defaultFile = new File("workers.yaml");
            if (defaultFile.exists()) {
                log.info("Using default worker file workers.yaml");
                arguments.workersFile = defaultFile;
            }
        }

        if (arguments.workersFile != null) {
            log.info("Reading workers list from {}", arguments.workersFile);
            arguments.workers = mapper.readValue(arguments.workersFile, Workers.class).workers;
        }

        // Dump configuration variables
        log.info("Starting benchmark with config: {}", writer.writeValueAsString(arguments));

        Map<String, Workload> workloads = new TreeMap<>();
        for (String path : arguments.workloads) {
            File file = new File(path);
            String name = file.getName().substring(0, file.getName().lastIndexOf('.'));

            workloads.put(name, mapper.readValue(file, Workload.class));
        }

        log.info("Workloads: {}", writer.writeValueAsString(workloads));

        Worker worker;

        if (arguments.workers != null && !arguments.workers.isEmpty()) {
            worker = new DistributedWorkersEnsemble(arguments.workers);
        } else {
            // Use local worker implementation
            worker = new LocalWorker();
        }

        workloads.forEach((workloadName, workload) -> {

            try {
                workload.validate();
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
                System.exit(-1);
            }

            arguments.drivers.forEach(driverConfig -> {
                try {
                    File driverConfigFile = new File(driverConfig);
                    DriverConfiguration driverConfiguration = mapper.readValue(driverConfigFile,
                            DriverConfiguration.class);

                    NamespaceMetadata metadata = provider
                            .getNamespaceMetaData(StringUtils.substringBetween(driverConfig, "/", "."));
                    driverConfiguration.namespaceName = metadata.NamespaceName;

                    log.info("--------------- WORKLOAD : {} --- DRIVER : {}---------------", workload.name,
                            driverConfiguration.name);
                    UUID uniqueRunId = UUID.randomUUID();
                    // Stop any left over workload
                    worker.stopAll();

                    worker.initializeDriver(new File(driverConfig));

                    WorkloadGenerator generator = new WorkloadGenerator(driverConfiguration.name, workload, worker, uniqueRunId);

                    TestResult result = generator.run();

                    //Enriching Metadata
                    result.testDetails.product = driverConfiguration.product;
                    result.testDetails.sku = driverConfiguration.sku;
                    result.testDetails.protocol = driverConfiguration.protocol;

                    Metadata testRunMetadata = Metadata.builder()
                            .workload(workload.name)
                            .payload(workload.payloadFile)
                            .namespaceName(driverConfiguration.namespaceName)
                            .topics(workload.topics)
                            .partitions(workload.partitionsPerTopic)
                            .consumerCount(workload.subscriptionsPerTopic)
                            .producerCount(workload.producersPerTopic)
                            .consumerGroups(workload.consumerPerSubscription)
                            .batchCount(driverConfiguration.batchCount)
                            .build();

                    testRunMetadata.partitions = workload.partitionsPerTopic;
                    result.testDetails.metadata = testRunMetadata;

                    String fileNamePrefix = arguments.output.length() > 0 ? arguments.output
                            : String.format("%s-%s-%s", workloadName, driverConfiguration.name,
                                    dateFormat.format(new Date()));

                    WriteTestResults(fileNamePrefix, result);
                    adapter.uploadDataToKustoCluster(fileNamePrefix);
                    log.info("Completed Execution of Run");
                    generator.close();
                } catch (Exception e) {
                    log.error("Failed to run the workload '{}' for driver '{}'", workload.name, driverConfig, e);
                } finally {
                    try {
                        worker.stopAll();
                    } catch (IOException e) {
                    }
                }
            });
        });
        worker.close();
        log.info("End of Benchmarking Run");
        System.exit(0);
    }

    private static void WriteTestResults(String fileNamePrefix, TestResult result) throws IOException {
        writer.writeValue(new File(fileNamePrefix + "-details.json"), result.testDetails);
        writer.writeValue(new File(fileNamePrefix + "-snapshot.json"), result.snapshotResultList);
        writer.writeValue(new File(fileNamePrefix + "-aggregate.json"), result.aggregateResult);
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

    private static final Logger log = LoggerFactory.getLogger(Benchmark.class);
}

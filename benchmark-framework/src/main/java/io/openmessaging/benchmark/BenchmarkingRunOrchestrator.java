package io.openmessaging.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.openmessaging.benchmark.driver.DriverConfiguration;
import io.openmessaging.benchmark.pojo.inputs.BenchmarkingRunArguments;
import io.openmessaging.benchmark.pojo.inputs.Workload;
import io.openmessaging.benchmark.pojo.output.Metadata;
import io.openmessaging.benchmark.pojo.output.TestResult;
import io.openmessaging.benchmark.worker.DistributedWorkersEnsemble;
import io.openmessaging.benchmark.worker.LocalWorker;
import io.openmessaging.benchmark.worker.Worker;
import org.apache.commons.text.CaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Properties;

import static java.util.stream.Collectors.toList;

public class BenchmarkingRunOrchestrator {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkingRunOrchestrator.class);
    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
    private static Worker benchmarkWorker;

    public static TestResult executeBenchmarkingRun(BenchmarkingRunArguments arguments) {
        log.info("--------------- WORKLOAD : {} --- DRIVER : {}---------------", arguments.getWorkload().name, arguments.getDriver().name);
        arguments.validate();
        stopDistributedWorkers();
        benchmarkWorker = getWorker(arguments);
        return runTestAndReturnResult(arguments);
    }

    private static TestResult runTestAndReturnResult(BenchmarkingRunArguments runArguments) {
        TestResult result = null;
        final DriverConfiguration driverConfiguration = runArguments.getDriver();

        //Augment workload with namespace information to be used by drivers.
        driverConfiguration.namespaceMetadata = runArguments.getNamespaceMetadata();

        final Workload workload = runArguments.getWorkload();
        try {
            log.info("Workload: {}", writer.writeValueAsString(workload));
            log.info("Driver: {}", writer.writeValueAsString(driverConfiguration));
            // Stop any leftover workload
            benchmarkWorker.stopAll();
            benchmarkWorker.initializeDriver(driverConfiguration);
            WorkloadGenerator generator = new WorkloadGenerator(runArguments, benchmarkWorker);
            result = generator.run();
            enrichTestResultWithMetadata(runArguments, result);
            log.info("Completed Execution of Run");
            generator.close();
        } catch (Exception e) {
            log.error("Failed to run the workload '{}' for driver '{}'", workload.name, driverConfiguration.name);
            log.error(String.valueOf(e));
            throw new RuntimeException(e);
        } finally {
            try {
                benchmarkWorker.stopAll();
                benchmarkWorker = null;
            } catch (IOException ignored) {
            }
        }

        return result;
    }

    private static void enrichTestResultWithMetadata(BenchmarkingRunArguments runArguments, TestResult result)
            throws IOException {
        final DriverConfiguration driverConfiguration = runArguments.getDriver();
        final Workload workload = runArguments.getWorkload();

        result.testDetails.testName = runArguments.getTestName();
        result.testDetails.testSuiteName = runArguments.getTestSuiteName();
        result.testDetails.product = driverConfiguration.product;
        result.testDetails.sku = driverConfiguration.sku;
        result.testDetails.protocol = driverConfiguration.protocol;

        // Fetch BatchSize in KB and BatchCount
        Properties producerProperties = new Properties();
        producerProperties.load(new StringReader(driverConfiguration.producerConfig));

        String batchSize = Optional.ofNullable(producerProperties.getProperty("batch.size")).orElse("1048576");
        batchSize = (Integer.parseInt(batchSize) / 1024) + "KB";
        int batchCount = Integer.parseInt(
                Optional.ofNullable(producerProperties.getProperty("batch.count")).orElse("1"));

        result.testDetails.metadata = Metadata.builder()
                .workload(workload.name) // Replacing workload name with test name
                .payload(workload.payloadFile)
                .namespaceName(runArguments.getNamespaceMetadata().namespaceName)
                .topics(workload.topics)
                .partitions(workload.partitionsPerTopic)
                .producerCount(workload.producersPerTopic)
                .consumerGroups(workload.subscriptionsPerTopic)
                .consumerCount(workload.consumerPerSubscription * workload.subscriptionsPerTopic)
                .batchCount(batchCount)
                .batchSize(batchSize)
                .tags(Optional.ofNullable(runArguments.getTags())
                        .orElse(new ArrayList<>())
                        .stream()
                        .map(s -> CaseUtils.toCamelCase(s, true))
                        .collect(toList()))
                .build();
    }

    private static Worker getWorker(BenchmarkingRunArguments arguments) {
        Worker worker;
        if (arguments.getWorkerAllocation().isRemoteWorkerRequired()) {
            worker = new DistributedWorkersEnsemble(arguments.getWorkerAllocation());
        } else {
            worker = new LocalWorker();
        }
        return worker;
    }

    public static void stopDistributedWorkers() {
        try {
            if(benchmarkWorker != null) {
                benchmarkWorker.stopAll();
                benchmarkWorker = null;
            }

            log.info("Stopped workers on thread shutdown");
        } catch (IOException e) {
            log.error("Could not stop workers before termination due to {}", e.getMessage());
        }
    }
}

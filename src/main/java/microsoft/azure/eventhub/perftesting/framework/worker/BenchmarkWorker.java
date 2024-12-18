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
package microsoft.azure.eventhub.perftesting.framework.worker;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.microsoft.applicationinsights.attach.ApplicationInsights;
import io.javalin.Javalin;
import org.apache.bookkeeper.stats.Stats;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A benchmark worker that listen for tasks to perform */
public class BenchmarkWorker {

  private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static final Logger log = LoggerFactory.getLogger(BenchmarkWorker.class);

  public static void main(String[] args) throws Exception {
    ApplicationInsights.attach();
    final Arguments arguments = new Arguments();
    JCommander jc = new JCommander(arguments);
    jc.setProgramName("benchmark-worker");

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

    Configuration conf = new CompositeConfiguration();
    conf.setProperty(Stats.STATS_PROVIDER_CLASS, PrometheusMetricsProvider.class.getName());
    conf.setProperty("prometheusStatsHttpPort", arguments.statsPort);
    Stats.loadStatsProvider(conf);
    StatsProvider provider = Stats.get();
    provider.start(conf);

    Runtime.getRuntime()
        .addShutdownHook(new Thread(() -> provider.stop(), "benchmark-worker-shutdown-thread"));

    // Dump configuration variables
    log.info("Starting benchmark with config: {}", writer.writeValueAsString(arguments));

    // Start web server
    Javalin app = Javalin.start(arguments.httpPort);

    new WorkerHandler(app, provider.getStatsLogger("benchmark"));
  }

  static class Arguments {

    @Parameter(
        names = {"-p", "--port"},
        description = "HTTP port to listen on")
    public int httpPort = 9000;

    @Parameter(
        names = {"-sp", "--stats-port"},
        description = "Stats port to listen on")
    public int statsPort = 9001;

    @Parameter(
        names = {"-h", "--help"},
        description = "Help message",
        help = true)
    boolean help;
  }
}

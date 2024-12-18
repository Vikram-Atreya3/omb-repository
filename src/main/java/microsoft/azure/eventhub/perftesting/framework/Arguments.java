package microsoft.azure.eventhub.perftesting.framework;

import com.beust.jcommander.Parameter;
import java.io.File;
import java.util.List;

class Arguments {

  @Parameter(
      names = {"-h", "--help"},
      description = "Help message",
      help = true)
  public boolean help;

  @Parameter(
      names = {"-d", "--drivers"},
      description = "Drivers list. eg.: pulsar/pulsar.yaml,kafka/kafka.yaml",
      required = true)
  public List<String> drivers;

  @Parameter(
      names = {"-w", "--workers"},
      description = "List of worker nodes. eg: http://1.2.3.4:8080,http://4.5.6.7:8080")
  public List<String> workers;

  @Parameter(
      names = {"-wf", "--workers-file"},
      description = "Path to a YAML file containing the list of workers addresses")
  public File workersFile;

  @Parameter(description = "Workloads", required = true)
  public List<String> workloads;

  @Parameter(
      names = {"-o", "--output"},
      description = "Output",
      required = false)
  public String output;

  @Parameter(
      names = {"-nm", "--namespaceMetadata"},
      description = "Metadata of Namespace",
      required = false)
  public String namespaceMetadata;

  @Parameter(
      names = {"-t", "--tags"},
      description = "Tags associated with the run")
  public List<String> tags;

  @Parameter(
      names = {"-p"},
      description = "Number of producer nodes out of the remote workers specified")
  public int producerWorkers = -1;

  @Parameter(
      names = {"-v", "--visualize"},
      arity = 1,
      description = "To control whether to use ADX DataSink or not")
  public boolean visualizeUsingKusto = true;
}

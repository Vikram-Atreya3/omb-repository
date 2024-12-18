package microsoft.azure.eventhub.perftesting.framework.driver.mgmtOperations;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import microsoft.azure.eventhub.perftesting.framework.driver.EventHubAdministrator;
import microsoft.azure.eventhub.perftesting.framework.driver.NamespaceMetadata;
import microsoft.azure.eventhub.perftesting.framework.pojo.inputs.Workload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MgmtOperationThread implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MgmtOperationThread.class);
  public static NamespaceMetadata namespaceMetadata;
  public static Workload workload;
  public EventHubAdministrator ehAdmin;
  private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  public abstract void run();

  public void scheduleOperationAndShutdown() throws InterruptedException {
    if (scheduler.isShutdown()) {
      scheduler = Executors.newScheduledThreadPool(1);
    }
    scheduler.schedule(
        this,
        workload.waitTimeBeforeMgmtOperation + workload.getWarmupTrafficDurationInMinutes(),
        TimeUnit.SECONDS);
    scheduler.shutdown();
  }

  public void shutdownScheduler() {
    if (scheduler.isTerminated()) return;
    log.info("Shutdown of scheduled executor of Mgmt Operation at end of orchestration");
    scheduler.shutdownNow();
  }
}

package com.annihilator.data.playground.core;

import static com.annihilator.data.playground.utility.DataPhantomUtility.*;

import com.annihilator.data.playground.cloud.aws.EMRService;
import com.annihilator.data.playground.config.ConcurrencyConfig;
import com.annihilator.data.playground.connector.MySQLConnector;
import com.annihilator.data.playground.db.AdhocLimitedInputDAO;
import com.annihilator.data.playground.db.PlaygroundDAO;
import com.annihilator.data.playground.db.PlaygroundRunHistoryDAO;
import com.annihilator.data.playground.db.TaskDAO;
import com.annihilator.data.playground.model.Playground;
import com.annihilator.data.playground.model.PlaygroundExecutionType;
import com.annihilator.data.playground.reconsilation.DataPhantomReconciliationManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataPhantomSchedulerAssistant implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DataPhantomSchedulerAssistant.class);

  private long lastUpdate = 0L;
  private PlaygroundDAO playgroundDAO;
  private TaskDAO taskDAO;
  private PlaygroundRunHistoryDAO historyDAO;
  private AdhocLimitedInputDAO adhocLimitedInputDAO;
  private PriorityQueue<Playground> playgroundQueue;
  private Map<String, Playground> playgroundMap;
  private ExecutorService executorService;
  private EMRService emrService;
  private Set<String> cancelPlaygroundRequestSet;
  private DataPhantomReconciliationManager reconciliationManager;
  private MySQLConnector mySQLConnector;
  private ConcurrencyConfig concurrencyConfig;

  public DataPhantomSchedulerAssistant(
      ConcurrencyConfig concurrencyConfig,
      PlaygroundDAO playgroundDAO,
      TaskDAO taskDAO,
      PlaygroundRunHistoryDAO historyDAO,
      AdhocLimitedInputDAO adhocLimitedInputDAO,
      EMRService emrService,
      ExecutorService executorService,
      DataPhantomReconciliationManager reconciliationManager,
      Set<String> cancelPlaygroundRequestSet,
      MySQLConnector mySQLConnector) {

    this.playgroundDAO = playgroundDAO;
    this.historyDAO = historyDAO;
    this.adhocLimitedInputDAO = adhocLimitedInputDAO;
    this.playgroundQueue = new PriorityQueue<>(new PlaygroundComparator());
    this.playgroundMap = new HashMap<>();
    this.executorService = executorService;
    this.taskDAO = taskDAO;
    this.emrService = emrService;
    this.cancelPlaygroundRequestSet = cancelPlaygroundRequestSet;
    this.reconciliationManager = reconciliationManager;
    this.mySQLConnector = mySQLConnector;
    this.concurrencyConfig = concurrencyConfig;
  }

  private void loadQueue() {

    logger.info("Loading execution queue");
    playgroundQueue.clear();
    playgroundMap.clear();

    Map<String, Playground> playgrounds = null;

    try {

      playgrounds = playgroundDAO.getAllPlaygrounds();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    if (playgrounds == null || playgrounds.isEmpty()) {

      logger.info("No playgrounds found for scheduling.");
      return;
    }

    for (Playground playground : playgrounds.values()) {

      playgroundQueue.add(playground);
      playgroundMap.put(playground.getId().toString(), playground);
      lastUpdate =
          Math.max(playground.getCreatedAt(), Math.max(lastUpdate, playground.getModifiedAt()));
    }
  }

  private void sleep(long millis) {

    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      logger.error("Scheduler interrupted during sleep", e);
      Thread.currentThread().interrupt();
    }
  }

  private boolean skipPlayground(Playground playground) {
    long executionTime = getNextExecutionTimeInMillis(playground.getCronExpression());
    long currentTime = getCurrentTimeInMillis();

    return currentTime - executionTime > concurrencyConfig.getPlaygroundExecutionGracePeriod()
        || (playground.getLastExecutedAt() > 0
            && System.currentTimeMillis() - playground.getLastExecutedAt()
                < concurrencyConfig.getPlaygroundMaxExecutionFrequency());
  }

  @Override
  public void run() {

    while (true) {

      loadQueue();

      while (!playgroundQueue.isEmpty()) {

        Playground nextPlayground = playgroundQueue.peek();

        if (skipPlayground(nextPlayground)) {

          playgroundQueue.poll();
          playgroundMap.remove(nextPlayground.getId().toString());
          continue;
        }

        long executionTime = getNextExecutionTimeInMillis(nextPlayground.getCronExpression());
        long currentTime = getCurrentTimeInMillis();

        long waitTime = executionTime - currentTime;

        long sleepTime = Math.min(waitTime, concurrencyConfig.getSchedulerSleepTime());

        if (sleepTime < 0) {
          sleepTime = 0;
        }

        logger.info(
            "Next playground: "
                + nextPlayground.getName()
                + " scheduled to run at: "
                + new Date(System.currentTimeMillis() + waitTime));
        sleep(sleepTime);

        if (executionTime > getCurrentTimeInMillis()) {
          checkUpdatedOrCreatedPlaygrounds();
          continue;
        }

        playgroundQueue.poll();
        playgroundMap.remove(nextPlayground.getId().toString());

        logger.info(
            "Executing playground: "
                + nextPlayground.getName()
                + " with cron: "
                + nextPlayground.getCronExpression());

        executorService.submit(
            new DataPhantomPlaygroundExecutor(
                nextPlayground,
                taskDAO,
                playgroundDAO,
                historyDAO,
                adhocLimitedInputDAO,
                emrService,
                PlaygroundExecutionType.SCHEDULED,
                reconciliationManager,
                cancelPlaygroundRequestSet,
                mySQLConnector,
                false,
                null));

        if (hasMoreExecutions(nextPlayground)) {

          playgroundQueue.add(nextPlayground);
          playgroundMap.put(nextPlayground.getId().toString(), nextPlayground);
        }
      }

      sleep(concurrencyConfig.getSchedulerSleepTime());
    }
  }

  private boolean hasMoreExecutions(Playground nextPlayground) {

    long newExecutionTime = getNextExecutionTimeInMillis(nextPlayground.getCronExpression());

    try {

      Playground playgroundFromDb = playgroundDAO.getPlaygroundById(nextPlayground.getId());

      if (Objects.isNull(playgroundFromDb)
          || !playgroundFromDb.getCronExpression().equals(nextPlayground.getCronExpression())) {
        return false;
      }

      return newExecutionTime > -1;
    } catch (SQLException e) {

      return false;
    }
  }

  private void checkUpdatedOrCreatedPlaygrounds() {

    Map<String, Playground> updatedPlaygrounds = null;

    try {
      updatedPlaygrounds = playgroundDAO.getPlaygroundsUpdatedOrCreatedAfter(lastUpdate);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    if (updatedPlaygrounds == null || updatedPlaygrounds.isEmpty()) {
      return;
    }

    logger.info(
        "Found "
            + updatedPlaygrounds.size()
            + " updated or created playgrounds after last update time: "
            + lastUpdate);

    for (Playground playground : updatedPlaygrounds.values()) {

      if (playgroundMap.containsKey(playground.getId().toString())) {

        playgroundQueue.remove(playgroundMap.get(playground.getId().toString()));
        playgroundMap.remove(playground.getId().toString());
      }

      playgroundQueue.add(playground);
      playgroundMap.put(playground.getId().toString(), playground);

      lastUpdate =
          Math.max(lastUpdate, Math.max(playground.getCreatedAt(), playground.getModifiedAt()));
    }
  }

  private static class PlaygroundComparator implements Comparator<Playground> {

    @Override
    public int compare(Playground p1, Playground p2) {

      String cron1 = p1.getCronExpression();
      String cron2 = p2.getCronExpression();

      long time1 = getNextExecutionTimeInMillis(cron1);
      long time2 = getNextExecutionTimeInMillis(cron2);

      return Long.compare(time1, time2);
    }
  }
}

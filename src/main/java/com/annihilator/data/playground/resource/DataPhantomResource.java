package com.annihilator.data.playground.resource;

import com.annihilator.data.playground.auth.DataPhantomUser;
import com.annihilator.data.playground.cloud.aws.EMRService;
import com.annihilator.data.playground.cloud.aws.S3Service;
import com.annihilator.data.playground.config.ConcurrencyConfig;
import com.annihilator.data.playground.config.DataPhantomConfig;
import com.annihilator.data.playground.connector.MySQLConnector;
import com.annihilator.data.playground.core.DataPhantomPlaygroundExecutor;
import com.annihilator.data.playground.core.DataPhantomSchedulerAssistant;
import com.annihilator.data.playground.db.*;
import com.annihilator.data.playground.model.*;
import com.annihilator.data.playground.reconsilation.AdaptiveCSVComparator;
import com.annihilator.data.playground.reconsilation.DataPhantomReconciliationManager;
import io.dropwizard.auth.Auth;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/data-phantom")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DataPhantomResource {

  private static final Logger logger = LoggerFactory.getLogger(DataPhantomResource.class);

  private final MetaDBConnection metaDBConnection;
  private final UserDAO userDAO;
  private final PlaygroundDAO playgroundDAO;
  private final TaskDAO taskDAO;
  private final AdhocLimitedInputDAO adhocLimitedInputDAO;
  private final PlaygroundRunHistoryDAO historyDAO;
  private final EMRService emrService;
  private final S3Service s3Service;
  private final DataPhantomReconciliationManager reconciliationManager;
  private final ReconciliationMappingDAO reconciliationMappingDAO;
  private final ReconciliationResultsDAO reconciliationResultsDAO;
  private final UDFDAO udfDAO;
  private final MySQLConnector mysqlConnector;
  private ExecutorService adhocExecutorService;
  private ExecutorService scheduledExecutorService;
  private Set<String> cancelPlaygroundRequestSet;
  private Set<String> runningReconciliationSet;
  private Map<String, java.util.concurrent.Future<?>> reconciliationFutures;
  private Thread schedulerThread;

  public DataPhantomResource(
      DataPhantomConfig config, io.dropwizard.core.setup.Environment environment) {

    this.metaDBConnection = new MetaDBConnection(config.getMetaStore(), environment);

    this.userDAO = new UserDAOImpl(metaDBConnection);
    this.playgroundDAO = new PlaygroundDAOImpl(metaDBConnection);
    this.taskDAO = new TaskDAOImpl(metaDBConnection);
    this.udfDAO = new UDFDAOImpl(metaDBConnection);
    this.historyDAO = new PlaygroundRunHistoryDAOImpl(metaDBConnection);
    this.adhocLimitedInputDAO = new AdhocLimitedInputDAOImpl(metaDBConnection);
    this.emrService =
        EMRService.getInstance(config.getConnector().getAwsEmrConfig(), udfDAO, taskDAO);
    this.s3Service = S3Service.getInstance(config.getConnector().getAwsEmrConfig());
    this.mysqlConnector =
        new MySQLConnector(
            config.getConnector().getMysql(),
            environment,
            s3Service,
            config.getConnector().getAwsEmrConfig().getS3PathPrefix());
    this.adhocExecutorService =
        Executors.newFixedThreadPool(config.getConcurrencyConfig().getAdHocThreadPoolSize());
    this.scheduledExecutorService =
        Executors.newFixedThreadPool(config.getConcurrencyConfig().getScheduledThreadPoolSize());
    this.cancelPlaygroundRequestSet = Collections.synchronizedSet(new HashSet<>());
    this.runningReconciliationSet = Collections.synchronizedSet(new HashSet<>());
    this.reconciliationFutures = Collections.synchronizedMap(new HashMap<>());
    this.reconciliationMappingDAO = new ReconciliationMappingDAOImpl(metaDBConnection);
    this.reconciliationResultsDAO = new ReconciliationResultsDAOImpl(metaDBConnection);
    this.reconciliationManager =
        new DataPhantomReconciliationManager(
            taskDAO,
            new AdaptiveCSVComparator(
                s3Service,
                reconciliationMappingDAO,
                reconciliationResultsDAO,
                taskDAO,
                config.getReconciliationConfig()));

    recover();

    startNewScheduler(config.getConcurrencyConfig());
  }

  private void recover() {
    try {
      List<Playground> runningPlaygrounds = playgroundDAO.getAllPlaygroundsByStatus(Status.RUNNING);

      logger.info(
          "Found " + runningPlaygrounds.size() + " playgrounds in RUNNING state for recovery.");

      for (Playground playground : runningPlaygrounds) {

        LimitedRunRequest limitedRunRequest =
            adhocLimitedInputDAO.getAdhocLimitedInputByRunId(
                playground.getCorrelationId().toString());

        if (limitedRunRequest != null) {
          logger.info(
              "Recovering limited adhoc run for playground {} with tasks: {}",
              playground.getName(),
              limitedRunRequest.getTasksToRun());
        }
        scheduledExecutorService.submit(
            new DataPhantomPlaygroundExecutor(
                playground,
                taskDAO,
                playgroundDAO,
                historyDAO,
                adhocLimitedInputDAO,
                emrService,
                PlaygroundExecutionType.RECOVERY,
                reconciliationManager,
                cancelPlaygroundRequestSet,
                mysqlConnector,
                limitedRunRequest != null,
                Optional.ofNullable(limitedRunRequest)
                    .map(LimitedRunRequest::getTasksToRun)
                    .orElse(null)));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void startNewScheduler(ConcurrencyConfig concurrencyConfig) {

    if (this.schedulerThread != null && this.schedulerThread.isAlive()) {
      logger.error("Scheduler thread is already running. Not starting a new one.");
      return;
    }

    this.schedulerThread =
        new Thread(
            new DataPhantomSchedulerAssistant(
                concurrencyConfig,
                this.playgroundDAO,
                this.taskDAO,
                this.historyDAO,
                this.adhocLimitedInputDAO,
                this.emrService,
                this.scheduledExecutorService,
                reconciliationManager,
                this.cancelPlaygroundRequestSet,
                this.mysqlConnector));
    schedulerThread.setDaemon(true);
    schedulerThread.start();
  }

  @GET
  @Path("/ping")
  public Map<String, Object> secureEndpoint(@Auth DataPhantomUser user) {
    Map<String, Object> response = new HashMap<>();
    response.put("alive", 1);
    return response;
  }

  @POST
  @Path("/reconciliation-run/{reconciliation_id}")
  public Response reconciliationRun(@PathParam("reconciliation_id") String reconciliationId) {
    try {
      UUID reconciliationUUID = UUID.fromString(reconciliationId);

      if (runningReconciliationSet.contains(reconciliationId)) {
        logger.info(
            "Reconciliation {} is already running, skipping duplicate request", reconciliationId);
        return Response.status(Response.Status.CONFLICT)
            .entity("Reconciliation request already submitted")
            .build();
      }

      runningReconciliationSet.add(reconciliationId);

      java.util.concurrent.Future<?> future =
          adhocExecutorService.submit(
              () -> {
                try {
                  logger.info(
                      "Reconciliation run started for reconciliation ID {}", reconciliationId);
                  reconciliationManager.startIndividualReconciliation(reconciliationUUID);
                  logger.info(
                      "Reconciliation run completed for reconciliation ID {}", reconciliationId);
                } catch (Exception e) {
                  logger.error(
                      "Error during reconciliation run for ID {}: {}",
                      reconciliationId,
                      e.getMessage(),
                      e);
                } finally {
                  runningReconciliationSet.remove(reconciliationId);
                  reconciliationFutures.remove(reconciliationId);
                  logger.info("Cleaned up tracking for reconciliation ID {}", reconciliationId);
                }
              });

      reconciliationFutures.put(reconciliationId, future);

      return Response.ok().entity("Reconciliation run started successfully").build();

    } catch (IllegalArgumentException e) {
      logger.error("Invalid reconciliation ID format: {}", reconciliationId);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Invalid reconciliation ID format")
          .build();
    } catch (Exception e) {
      logger.error(
          "Unexpected error in reconciliation run for ID {}: {}",
          reconciliationId,
          e.getMessage(),
          e);
      runningReconciliationSet.remove(reconciliationId);
      reconciliationFutures.remove(reconciliationId);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to start reconciliation run: " + e.getMessage())
          .build();
    }
  }

  @POST
  @Path("/adhoc-run/{playground_id}")
  public Response adhocRun(@PathParam("playground_id") String playgroundId) {
    try {

      UUID playgroundUUID = UUID.fromString(playgroundId);
      Playground playground = playgroundDAO.getPlaygroundById(playgroundUUID);
      if (playground == null) {
        logger.error("Playground not found for adhoc run: {}", playgroundId);
        return Response.status(Response.Status.NOT_FOUND).entity("Playground not found").build();
      }

      adhocExecutorService.submit(
          new DataPhantomPlaygroundExecutor(
              playground,
              taskDAO,
              playgroundDAO,
              historyDAO,
              adhocLimitedInputDAO,
              emrService,
              PlaygroundExecutionType.AD_HOC,
              reconciliationManager,
              cancelPlaygroundRequestSet,
              mysqlConnector,
              false,
              null));

      logger.info("Adhoc run started for playground {}", playground.getName());

      return Response.ok().entity("Adhoc run started successfully").build();
    } catch (Exception e) {

      logger.error("Error in adhoc run for playground {}", playgroundId, e);

      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to start adhoc run: " + e.getMessage())
          .build();
    }
  }

  @GET
  @Path("/task/fields/{task_id}")
  public Response getTaskSelectedFields(@PathParam("task_id") String taskId) {
    try {
      List<String> selectedFields = reconciliationManager.getSelectedFields(taskId);

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("selectedFields", selectedFields);
      response.put("count", selectedFields.size());

      return Response.ok().entity(response).build();
    } catch (Exception e) {
      logger.error("Error retrieving selected fields for task {}: {}", taskId, e.getMessage(), e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to retrieve selected fields")
          .build();
    }
  }

  @POST
  @Path("/limited-adhoc-run")
  public Response adhocLimitedRun(LimitedRunRequest limitedRunRequest) {
    try {

      logger.info("Received limited adhoc run request: {}", limitedRunRequest);

      if (Objects.isNull(limitedRunRequest)
          || limitedRunRequest.getTasksToRun() == null
          || limitedRunRequest.getTasksToRun().isEmpty()
          || limitedRunRequest.getPlaygroundId() == null
          || limitedRunRequest.getPlaygroundId().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Invalid request: playground ID and tasks to run cannot be empty")
            .build();
      }

      UUID playgroundUUID = UUID.fromString(limitedRunRequest.getPlaygroundId());
      Playground playground = playgroundDAO.getPlaygroundById(playgroundUUID);

      if (playground == null) {
        logger.error("Playground not found for limited adhoc run: {}", playgroundUUID);
        return Response.status(Response.Status.NOT_FOUND).entity("Playground not found").build();
      }

      adhocExecutorService.submit(
          new DataPhantomPlaygroundExecutor(
              playground,
              taskDAO,
              playgroundDAO,
              historyDAO,
              adhocLimitedInputDAO,
              emrService,
              PlaygroundExecutionType.AD_HOC,
              reconciliationManager,
              cancelPlaygroundRequestSet,
              mysqlConnector,
              true,
              limitedRunRequest.getTasksToRun()));

      logger.info("Limited adhoc run started for playground {}", playground.getName());

      return Response.ok().entity("Limited adhoc run started successfully").build();
    } catch (Exception e) {

      logger.error(
          "Error in limited adhoc run for playground {}", limitedRunRequest.getPlaygroundId(), e);

      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to start limited adhoc run: " + e.getMessage())
          .build();
    }
  }

  @POST
  @Path("/user")
  public Response createUser(User user) {
    try {
      userDAO.createUser(user);

      Map<String, Object> response = new HashMap<>();
      response.put("message", "User created successfully");
      response.put("user", user);

      return Response.ok().entity(response).build();
    } catch (SQLException e) {
      logger.error("Error creating user", e);

      if ("23000".equals(e.getSQLState()) && e.getErrorCode() == 1062) {
        return Response.status(Response.Status.CONFLICT)
            .entity("User with the same ID already exists.")
            .build();
      }

      return Response.serverError().entity("Failed to create user: " + e.getMessage()).build();
    }
  }

  @DELETE
  @Path("/user/{user_id}")
  public Response deleteUser(@PathParam("user_id") String userId) {
    try {
      userDAO.deleteUser(userId);
      return Response.ok().entity("User with user name" + userId + " deleted successfully").build();
    } catch (SQLException e) {
      logger.error("Error deleting user", e);
      return Response.serverError().entity("Failed to delete user with userId " + userId).build();
    }
  }

  @GET
  @Path("/playground/{user_id}")
  public Response getPlaygrounds(@PathParam("user_id") String userId) {

    try {
      Map<String, Object> playgrounds = playgroundDAO.getPlaygrounds(userId);

      return Response.ok().entity(playgrounds).build();
    } catch (SQLException e) {
      logger.error("Error creating playground", e);
      return Response.serverError().entity("Failed to create playground").build();
    }
  }

  @POST
  @Path("/playground")
  public Response createPlayground(Playground playground) {

    try {
      playgroundDAO.createPlayground(playground);
      return Response.ok().entity(playground).build();
    } catch (SQLException e) {
      logger.error("Error creating playground", e);
      return Response.serverError().entity("Failed to create playground").build();
    }
  }

  @PUT
  @Path("/playground/update")
  public Response updatePlayground(Playground playground) {
    try {

      playgroundDAO.updatePlayground(
          playground.getId(), playground.getName(), playground.getCronExpression());

      return Response.ok().entity(playground).build();
    } catch (SQLException e) {
      logger.error("Error updating playground", e);
      return Response.serverError().entity("Failed to update playground").build();
    }
  }

  @POST
  @Path("/playground/cancel/{id}")
  public Response cancelPlaygroundRun(@PathParam("id") UUID id) {

    try {
      Playground playground = playgroundDAO.getPlaygroundById(id);

      if (Objects.isNull(playground.getCurrentStatus())
          || !playground.getCurrentStatus().equals(Status.RUNNING)) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Playground is not in RUNNING state, cannot cancel.")
            .build();
      }

      cancelPlaygroundRequestSet.add(id.toString());

      return Response.ok().entity("Playground cancellation requested successfully").build();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @DELETE
  @Path("/playground/{id}")
  public Response deletePlayground(@PathParam("id") UUID id) {
    try {
      playgroundDAO.deletePlayground(id);
      return Response.ok().build();
    } catch (SQLException e) {
      logger.error("Error deleting playground", e);
      return Response.serverError().entity("Failed to delete playground").build();
    }
  }

  @POST
  @Path("/task")
  public Response createTask(Task task) {
    try {
      taskDAO.createTaskAndUpdatePlayground(task);
      return Response.ok().entity(task).build();
    } catch (SQLException e) {
      logger.error("Error creating task", e);
      return Response.serverError().entity("Failed to create task").build();
    }
  }

  @GET
  @Path("/playground/{playgroundId}/tasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPlaygroundTasks(@PathParam("playgroundId") String playgroundId) {
    try {
      UUID playgroundUUID;
      try {
        playgroundUUID = UUID.fromString(playgroundId);
      } catch (IllegalArgumentException e) {
        logger.error("Invalid playground ID format: {}", playgroundId);
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Invalid playground ID format")
            .build();
      }

      List<Task> tasks = taskDAO.findTasksByPlaygroundRecursively(playgroundUUID);

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("tasks", tasks);
      response.put("count", tasks.size());

      return Response.ok().entity(response).build();

    } catch (SQLException e) {
      logger.error("Error fetching tasks for playground {}: {}", playgroundId, e.getMessage(), e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to fetch tasks")
          .build();
    }
  }

  @GET
  @Path("/preview/{s3Location}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getS3FilePreview(@PathParam("s3Location") String s3) {

    try {
      List<String> previewLines = s3Service.readOutputPreview(s3);

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("preview", previewLines);
      response.put("lineCount", previewLines.size());

      return Response.ok().entity(response).build();

    } catch (Exception e) {
      logger.error("Error fetching S3 file preview for {}: {}", s3, e.getMessage(), e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to fetch S3 file preview")
          .build();
    }
  }

  @PUT
  @Path("/task/query")
  public Response updateTaskQuery(Task task) {
    try {

      taskDAO.updateTaskQueryAndPlayground(
          task.getId(), task.getQuery(), task.getPlaygroundId(), task.getUdfIds());

      return Response.ok().build();
    } catch (SQLException e) {
      logger.error("Error updating task query", e);
      return Response.serverError().entity("Failed to update task query").build();
    }
  }

  @DELETE
  @Path("/task/{taskId}")
  public Response deleteTask(@PathParam("taskId") String taskId) {
    try {
      taskDAO.deleteTaskAndUpdatePlayground(taskId);
      return Response.ok().build();
    } catch (SQLException e) {
      logger.error("Error deleting task", e);
      return Response.serverError().entity("Failed to delete task").build();
    }
  }

  @GET
  @Path("/playground/{playgroundId}/run-history")
  public Response getPlaygroundRunHistory(
      @PathParam("playgroundId") String playgroundId, @QueryParam("limit") int limit) {

    try {
      UUID playgroundUuid = UUID.fromString(playgroundId);

      if (limit <= 0 || limit > 100) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Limit must be between 1 and 100 (number of runs)")
            .build();
      }

      Map<String, Object> runHistory =
          historyDAO.getRunHistoryByPlaygroundId(playgroundUuid, limit);

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("data", runHistory);

      return Response.ok().entity(response).build();

    } catch (IllegalArgumentException e) {
      logger.error("Invalid playground ID format: {}", playgroundId, e);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Invalid playground ID format")
          .build();
    } catch (SQLException e) {
      logger.error(
          "Error fetching run history for playground {}: {}", playgroundId, e.getMessage(), e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to fetch run history")
          .build();
    }
  }

  @POST
  @Path("/reconciliation-cancel/{reconciliation_id}")
  public Response cancelReconciliationRun(@PathParam("reconciliation_id") String reconciliationId) {
    try {
      if (!runningReconciliationSet.contains(reconciliationId)) {
        logger.info("Reconciliation {} is not running, cannot cancel", reconciliationId);
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Reconciliation is not currently running")
            .build();
      }

      java.util.concurrent.Future<?> future = reconciliationFutures.get(reconciliationId);
      if (future != null && future.cancel(true)) {
        logger.info("Successfully cancelled reconciliation {}", reconciliationId);
        return Response.ok().entity("Reconciliation cancellation requested successfully").build();
      } else {
        logger.warn(
            "Failed to cancel reconciliation {} - future not found or already completed",
            reconciliationId);
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity("Failed to cancel reconciliation")
            .build();
      }

    } catch (Exception e) {
      logger.error("Error cancelling reconciliation {}: {}", reconciliationId, e.getMessage(), e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Failed to cancel reconciliation: " + e.getMessage())
          .build();
    }
  }

  @POST
  @Path("/reconciliation-mapping")
  public Response createReconciliationMapping(ReconciliationMappingRequest request) {
    try {
      if (request.getPlaygroundId() == null || request.getPlaygroundId().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"Playground ID is required\"}")
            .build();
      }
      if (request.getLeftTableId() == null || request.getLeftTableId().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"Left table ID is required\"}")
            .build();
      }
      if (request.getRightTableId() == null || request.getRightTableId().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"Right table ID is required\"}")
            .build();
      }
      if (request.getMap() == null || request.getMap().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"Map is required\"}")
            .build();
      }

      reconciliationMappingDAO.createReconciliationMapping(
          request.getPlaygroundId(),
          request.getLeftTableId(),
          request.getRightTableId(),
          request.getMap());

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("message", "Reconciliation mapping created successfully");

      return Response.status(Response.Status.CREATED).entity(response).build();

    } catch (SQLException e) {
      logger.error("Error creating reconciliation mapping", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("{\"error\": \"Failed to create reconciliation mapping\"}")
          .build();
    } catch (IllegalArgumentException e) {
      logger.error("Invalid input for reconciliation mapping", e);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("{\"error\": \"" + e.getMessage() + "\"}")
          .build();
    }
  }

  @GET
  @Path("/reconciliation-mapping/playground/{playgroundId}")
  public Response getReconciliationMappingsByPlayground(
      @PathParam("playgroundId") String playgroundId) {
    try {
      List<Reconciliation> mappings =
          reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(playgroundId);

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("data", mappings);

      return Response.ok().entity(response).build();

    } catch (SQLException e) {
      logger.error("Error fetching reconciliation mappings for playground: {}", playgroundId, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("{\"error\": \"Failed to fetch reconciliation mappings\"}")
          .build();
    } catch (IllegalArgumentException e) {
      logger.error("Invalid playground ID: {}", playgroundId, e);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("{\"error\": \"" + e.getMessage() + "\"}")
          .build();
    }
  }

  @GET
  @Path("/reconciliation-mapping/{reconciliationId}")
  public Response getReconciliationMappingById(
      @PathParam("reconciliationId") String reconciliationId) {
    try {
      Reconciliation mapping =
          reconciliationMappingDAO.findReconciliationMappingById(reconciliationId);

      if (mapping == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity("{\"error\": \"Reconciliation mapping not found\"}")
            .build();
      }

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("data", mapping);

      return Response.ok().entity(response).build();

    } catch (SQLException e) {
      logger.error("Error fetching reconciliation mapping: {}", reconciliationId, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("{\"error\": \"Failed to fetch reconciliation mapping\"}")
          .build();
    } catch (IllegalArgumentException e) {
      logger.error("Invalid reconciliation ID: {}", reconciliationId, e);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("{\"error\": \"" + e.getMessage() + "\"}")
          .build();
    }
  }

  @PUT
  @Path("/reconciliation-mapping/{reconciliationId}")
  public Response updateReconciliationMapping(
      @PathParam("reconciliationId") String reconciliationId,
      ReconciliationMappingUpdateRequest request) {
    try {
      if (request.getMap() == null || request.getMap().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"Map is required\"}")
            .build();
      }

      reconciliationMappingDAO.updateReconciliationMapping(reconciliationId, request.getMap());

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("message", "Reconciliation mapping updated successfully");

      return Response.ok().entity(response).build();

    } catch (SQLException e) {
      logger.error("Error updating reconciliation mapping: {}", reconciliationId, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("{\"error\": \"Failed to update reconciliation mapping\"}")
          .build();
    } catch (IllegalArgumentException e) {
      logger.error("Invalid input for reconciliation mapping update", e);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("{\"error\": \"" + e.getMessage() + "\"}")
          .build();
    }
  }

  @DELETE
  @Path("/reconciliation-mapping/{reconciliationId}")
  public Response deleteReconciliationMapping(
      @PathParam("reconciliationId") String reconciliationId) {
    try {
      reconciliationMappingDAO.deleteReconciliationMappingById(reconciliationId);

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("message", "Reconciliation mapping deleted successfully");

      return Response.ok().entity(response).build();

    } catch (SQLException e) {
      logger.error("Error deleting reconciliation mapping: {}", reconciliationId, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("{\"error\": \"Failed to delete reconciliation mapping\"}")
          .build();
    } catch (IllegalArgumentException e) {
      logger.error("Invalid reconciliation ID: {}", reconciliationId, e);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("{\"error\": \"" + e.getMessage() + "\"}")
          .build();
    }
  }

  @GET
  @Path("/reconciliation-result/{reconciliationId}")
  public Response getReconciliationResult(@PathParam("reconciliationId") String reconciliationId) {
    try {
      ReconciliationResultResponse result =
          reconciliationResultsDAO.getReconciliationResult(reconciliationId);

      if (result == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity("{\"error\": \"Reconciliation result not found\"}")
            .build();
      }

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("data", result);

      return Response.ok().entity(response).build();

    } catch (SQLException e) {
      logger.error("Error fetching reconciliation result: {}", reconciliationId, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("{\"error\": \"Failed to fetch reconciliation result\"}")
          .build();
    } catch (IllegalArgumentException e) {
      logger.error("Invalid reconciliation ID: {}", reconciliationId, e);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("{\"error\": \"" + e.getMessage() + "\"}")
          .build();
    }
  }

  @GET
  @Path("/reconciliation-status/{reconciliationId}")
  public Response getReconciliationStatus(@PathParam("reconciliationId") String reconciliationId) {
    try {
      Map<String, Object> response = new HashMap<>();
      response.put("success", true);

      if (runningReconciliationSet.contains(reconciliationId)) {
        response.put("status", "RUNNING");
        response.put("message", "Reconciliation is currently running");
      } else {
        try {
          ReconciliationResultResponse result =
              reconciliationResultsDAO.getReconciliationResult(reconciliationId);
          if (result != null) {
            response.put("status", result.getStatus()); // SUCCESS or FAILED
            response.put("executionTimestamp", result.getExecutionTimestamp());
            response.put("reconciliationMethod", result.getReconciliationMethod());
          } else {
            // No record in database means never been started
            response.put("status", "FAILED");
            response.put("message", "Reconciliation has not been started yet");
          }
        } catch (SQLException e) {
          logger.error(
              "Error fetching reconciliation result from database: {}", reconciliationId, e);
          response.put("status", "FAILED");
          response.put("message", "Unable to determine status");
        }
      }

      return Response.ok().entity(response).build();

    } catch (IllegalArgumentException e) {
      logger.error("Invalid reconciliation ID: {}", reconciliationId, e);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("{\"error\": \"" + e.getMessage() + "\"}")
          .build();
    }
  }

  @POST
  @Path("/udf")
  public Response createUDF(UDF udf) {

    try {
      if (udf.getUserId() == null || udf.getUserId().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"User ID is required\"}")
            .build();
      }
      if (udf.getName() == null || udf.getName().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"UDF name is required\"}")
            .build();
      }
      if (udf.getFunctionName() == null || udf.getFunctionName().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"Function name is required\"}")
            .build();
      }
      if (udf.getJarS3Path() == null || udf.getJarS3Path().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"JAR S3 path is required\"}")
            .build();
      }
      if (udf.getClassName() == null || udf.getClassName().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"Class name is required\"}")
            .build();
      }
      if (udf.getParameterTypes() == null || udf.getParameterTypes().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"Parameter types are required\"}")
            .build();
      }
      if (udf.getReturnType() == null || udf.getReturnType().trim().isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("{\"error\": \"Return type is required\"}")
            .build();
      }

      udfDAO.createUDF(udf);

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("message", "UDF created successfully");
      response.put("udf", udf);

      return Response.status(Response.Status.CREATED).entity(response).build();

    } catch (SQLException e) {
      logger.error("Error creating UDF", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("{\"error\": \"Failed to create UDF\"}")
          .build();
    }
  }

  @GET
  @Path("/udf/user/{userId}")
  public Response getUDFsByUserId(@PathParam("userId") String userId) {
    try {
      List<UDF> udfs = udfDAO.getUDFsByUserId(userId);

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("udfs", udfs);
      response.put("count", udfs.size());

      return Response.ok().entity(response).build();

    } catch (SQLException e) {
      logger.error("Error fetching UDFs for user: {}", userId, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("{\"error\": \"Failed to fetch UDFs\"}")
          .build();
    }
  }

  @DELETE
  @Path("/udf/{udfId}")
  public Response deleteUDFById(@PathParam("udfId") String udfId) {
    try {
      udfDAO.deleteUDFById(udfId);

      Map<String, Object> response = new HashMap<>();
      response.put("success", true);
      response.put("message", "UDF deleted successfully");

      return Response.ok().entity(response).build();

    } catch (SQLException e) {
      logger.error("Error deleting UDF: {}", udfId, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("{\"error\": \"Failed to delete UDF\"}")
          .build();
    }
  }
}

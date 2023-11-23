/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.runner;

import static org.apache.dolphinscheduler.common.constants.Constants.DEFAULT_WORKER_GROUP;
import static org.apache.dolphinscheduler.common.constants.Constants.SINGLE_SLASH;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import lombok.SneakyThrows;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.constants.TenantConstants;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.TaskEventType;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.FileUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.dao.entity.Environment;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelation;
import org.apache.dolphinscheduler.dao.entity.Resource;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.mapper.ProcessTaskRelationMapper;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.plugin.storage.api.StorageOperate;
import org.apache.dolphinscheduler.plugin.task.api.*;
import org.apache.dolphinscheduler.plugin.task.api.enums.Direct;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.log.TaskInstanceLogHeader;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.ParametersNode;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.ResourceParametersHelper;
import org.apache.dolphinscheduler.plugin.task.api.stream.StreamTask;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.ParameterUtils;
import org.apache.dolphinscheduler.remote.command.MessageType;
import org.apache.dolphinscheduler.remote.command.task.TaskExecuteRunningMessageAck;
import org.apache.dolphinscheduler.remote.command.task.TaskExecuteStartMessage;
import org.apache.dolphinscheduler.server.master.builder.TaskExecutionContextBuilder;
import org.apache.dolphinscheduler.server.master.cache.StreamTaskInstanceExecCacheManager;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.event.StateEventHandleError;
import org.apache.dolphinscheduler.server.master.event.StateEventHandleException;
import org.apache.dolphinscheduler.server.master.exception.TaskExecuteRunnableCreateException;
import org.apache.dolphinscheduler.server.master.exception.TaskExecutionContextCreateException;
import org.apache.dolphinscheduler.server.master.metrics.TaskMetrics;
import org.apache.dolphinscheduler.server.master.processor.queue.TaskEvent;
import org.apache.dolphinscheduler.server.master.runner.dispatcher.WorkerTaskDispatcher;
import org.apache.dolphinscheduler.server.master.runner.execute.DefaultTaskExecuteRunnable;
import org.apache.dolphinscheduler.server.master.runner.execute.DefaultTaskExecuteRunnableFactory;
import org.apache.dolphinscheduler.server.master.runner.execute.MasterTaskExecutionContextHolder;
import org.apache.dolphinscheduler.server.master.runner.execute.TaskExecutionContextFactory;
import org.apache.dolphinscheduler.server.master.utils.TaskUtils;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.spi.enums.ResourceType;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

/**
 * stream task execute
 */
@Slf4j
public class StreamTaskExecuteRunnable implements Runnable {

    protected MasterConfig masterConfig;

    protected ProcessService processService;

    protected TaskInstanceDao taskInstanceDao;

    protected DefaultTaskExecuteRunnableFactory defaultTaskExecuteRunnableFactory;

    protected WorkerTaskDispatcher workerTaskDispatcher;

    protected ProcessTaskRelationMapper processTaskRelationMapper;

    protected TaskPluginManager taskPluginManager;

    private StreamTaskInstanceExecCacheManager streamTaskInstanceExecCacheManager;

    protected TaskDefinition taskDefinition;

    protected TaskInstance taskInstance;

    protected ProcessDefinition processDefinition;

    protected TaskExecuteStartMessage taskExecuteStartMessage;

    protected TaskExecutionContextFactory taskExecutionContextFactory;

    protected TaskExecutionContext taskExecutionContext;

    protected StorageOperate storageOperate;

    protected @Nullable
    AbstractTask task;


    /**
     * task event queue
     */
    private final ConcurrentLinkedQueue<TaskEvent> taskEvents = new ConcurrentLinkedQueue<>();

    private TaskRunnableStatus taskRunnableStatus = TaskRunnableStatus.CREATED;

    public StreamTaskExecuteRunnable(TaskDefinition taskDefinition, TaskExecuteStartMessage taskExecuteStartMessage, StorageOperate storageOperate) {
        this.processService = SpringApplicationContext.getBean(ProcessService.class);
        this.masterConfig = SpringApplicationContext.getBean(MasterConfig.class);
        this.workerTaskDispatcher = SpringApplicationContext.getBean(WorkerTaskDispatcher.class);
        this.taskPluginManager = SpringApplicationContext.getBean(TaskPluginManager.class);
        this.processTaskRelationMapper = SpringApplicationContext.getBean(ProcessTaskRelationMapper.class);
        this.taskInstanceDao = SpringApplicationContext.getBean(TaskInstanceDao.class);
        this.streamTaskInstanceExecCacheManager =
                SpringApplicationContext.getBean(StreamTaskInstanceExecCacheManager.class);
        this.taskDefinition = taskDefinition;
        this.taskExecuteStartMessage = taskExecuteStartMessage;
        this.taskExecutionContextFactory = SpringApplicationContext.getBean(TaskExecutionContextFactory.class);
        this.defaultTaskExecuteRunnableFactory = SpringApplicationContext.getBean(DefaultTaskExecuteRunnableFactory.class);
        this.storageOperate = storageOperate;
    }

    public TaskInstance getTaskInstance() {
        return taskInstance;
    }

    @Override
    public void run() {
        log.info("## storageOperate is null : {}", ObjectUtils.isEmpty(storageOperate));
        // submit task
        processService.updateTaskDefinitionResources(taskDefinition);
        log.info("## taskDefinition is:{}", JSONUtils.toJsonString(taskDefinition));
        taskInstance = newTaskInstance(taskDefinition);
        log.info("## taskInstance is:{}", JSONUtils.toJsonString(taskInstance));
        taskInstanceDao.upsertTaskInstance(taskInstance);
        List<ProcessTaskRelation> processTaskRelationList =
                processTaskRelationMapper.queryByTaskCode(taskDefinition.getCode());
        long processDefinitionCode = processTaskRelationList.get(0).getProcessDefinitionCode();
        int processDefinitionVersion = processTaskRelationList.get(0).getProcessDefinitionVersion();
        processDefinition = processService.findProcessDefinition(processDefinitionCode, processDefinitionVersion);

        taskExecutionContext = getTaskExecutionContext(taskInstance);
        TaskInstanceLogHeader.printInitializeTaskContextHeader();
        initializeTask();
        // add cache
        streamTaskInstanceExecCacheManager.cache(taskInstance.getId(), this);
        TaskExecutionContextCacheManager.cacheTaskExecutionContext(taskExecutionContext);


        try {
            // DefaultTaskExecuteRunnable taskExecuteRunnable =
            //         defaultTaskExecuteRunnableFactory.createTaskExecuteRunnable(taskInstance);
            // workerTaskDispatcher.dispatchTask(taskExecuteRunnable);
            TaskInstanceLogHeader.printLoadTaskInstancePluginHeader();
            beforeExecute();

            TaskCallBack taskCallBack = StreamTaskCallBack.builder()
                    .taskExecutionContext(taskExecutionContext)
                    .streamTaskExecuteRunnable(this).build();
            TaskInstanceLogHeader.printExecuteTaskHeader();
            taskRunnableStatus = TaskRunnableStatus.STARTED;
            executeTask(taskCallBack);

            TaskInstanceLogHeader.printFinalizeTaskHeader();
            afterExecute();
        } catch (Exception e) {
            // log.error("Master dispatch task to worker error, taskInstanceName: {}", taskInstance.getName(), e);
            log.error(String.format("Master dispatch task to worker error, taskInstanceName: %s", taskInstance.getName()), e);
            taskInstance.setState(TaskExecutionStatus.FAILURE);
            taskInstanceDao.upsertTaskInstance(taskInstance);
            return;
        }
        // set started flag

        log.info("Master success dispatch task to worker, taskInstanceName: {}, worker: {}", taskInstance.getId(),
                taskInstance.getHost());
    }

    public boolean isStart() {
        return TaskRunnableStatus.STARTED == taskRunnableStatus;
    }

    public boolean addTaskEvent(TaskEvent taskEvent) {
        if (taskInstance.getId() != taskEvent.getTaskInstanceId()) {
            log.info("state event would be abounded, taskInstanceId:{}, eventType:{}, state:{}",
                    taskEvent.getTaskInstanceId(), taskEvent.getEvent(), taskEvent.getState());
            return false;
        }
        taskEvents.add(taskEvent);
        return true;
    }

    public int eventSize() {
        return this.taskEvents.size();
    }

    /**
     * handle event
     */
    public void handleEvents() {
        if (!isStart()) {
            log.info(
                    "The stream task instance is not started, will not handle its state event, current state event size: {}",
                    taskEvents.size());
            return;
        }
        TaskEvent taskEvent = null;
        while (!this.taskEvents.isEmpty()) {
            try {
                taskEvent = this.taskEvents.poll();
                LogUtils.setTaskInstanceIdMDC(taskEvent.getTaskInstanceId());

                log.info("Begin to handle state event, {}", taskEvent);
                this.handleTaskEvent(taskEvent);
            } catch (StateEventHandleError stateEventHandleError) {
                log.error("State event handle error, will remove this event: {}", taskEvent, stateEventHandleError);
                this.taskEvents.remove(taskEvent);
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS);
            } catch (StateEventHandleException stateEventHandleException) {
                log.error("State event handle error, will retry this event: {}",
                        taskEvent,
                        stateEventHandleException);
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS);
            } catch (Exception e) {
                // we catch the exception here, since if the state event handle failed, the state event will still keep
                // in the stateEvents queue.
                log.error("State event handle error, get a unknown exception, will retry this event: {}",
                        taskEvent,
                        e);
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS);
            } finally {
                LogUtils.removeWorkflowAndTaskInstanceIdMDC();
            }
        }
    }

    public TaskInstance newTaskInstance(TaskDefinition taskDefinition) {
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setTaskCode(taskDefinition.getCode());
        taskInstance.setTaskDefinitionVersion(taskDefinition.getVersion());
        taskInstance.setName(StringUtils.isNotBlank(taskExecuteStartMessage.getName()) ? taskExecuteStartMessage.getName() : taskDefinition.getName());
        // task instance state
        taskInstance.setState(TaskExecutionStatus.SUBMITTED_SUCCESS);
        // set process instance id to 0
        taskInstance.setProcessInstanceId(0);
        taskInstance.setProjectCode(taskDefinition.getProjectCode());
        // task instance type
        taskInstance.setTaskType(taskDefinition.getTaskType().toUpperCase());
        // task instance whether alert
        taskInstance.setAlertFlag(Flag.NO);

        // task instance start time
        taskInstance.setStartTime(null);

        // task instance flag
        taskInstance.setFlag(Flag.YES);

        // task instance current retry times
        taskInstance.setRetryTimes(0);
        taskInstance.setMaxRetryTimes(taskDefinition.getFailRetryTimes());
        taskInstance.setRetryInterval(taskDefinition.getFailRetryInterval());

        ObjectNode taskParams = JSONUtils.parseObject(taskDefinition.getTaskParams());
        if (MapUtils.isNotEmpty(taskExecuteStartMessage.getStartParams()) && taskExecuteStartMessage.getStartParams().containsKey("runArgs")) {
            taskParams.set("runArgs", JSONUtils.parseObject(taskExecuteStartMessage.getStartParams().get("runArgs")));
        }
        // set task param
        taskInstance.setTaskParams(JSONUtils.toJsonString(taskParams));

        ObjectNode paramMap = JSONUtils.parseObject(taskDefinition.getTaskParams());
        if (paramMap.has("mainJar")) {
            Map<String, String> resourcesMap = new HashMap<>();
            resourcesMap.put(paramMap.get("mainJar").get("resourceName").asText(), "");
            taskInstance.setResources(resourcesMap);
        }

        // set task group and priority
        taskInstance.setTaskGroupId(taskDefinition.getTaskGroupId());
        taskInstance.setTaskGroupPriority(taskDefinition.getTaskGroupPriority());

        // set task cpu quota and max memory
        taskInstance.setCpuQuota(taskDefinition.getCpuQuota());
        taskInstance.setMemoryMax(taskDefinition.getMemoryMax());

        // task instance priority
        taskInstance.setTaskInstancePriority(Priority.MEDIUM);
        if (taskDefinition.getTaskPriority() != null) {
            taskInstance.setTaskInstancePriority(taskDefinition.getTaskPriority());
        }

        // delay execution time
        taskInstance.setDelayTime(taskDefinition.getDelayTime());

        // task dry run flag
        taskInstance.setDryRun(taskExecuteStartMessage.getDryRun());

        taskInstance.setWorkerGroup(StringUtils.isBlank(taskDefinition.getWorkerGroup()) ? DEFAULT_WORKER_GROUP
                : taskDefinition.getWorkerGroup());
        taskInstance.setEnvironmentCode(
                taskDefinition.getEnvironmentCode() == 0 ? -1 : taskDefinition.getEnvironmentCode());

        if (!taskInstance.getEnvironmentCode().equals(-1L)) {
            Environment environment = processService.findEnvironmentByCode(taskInstance.getEnvironmentCode());
            if (Objects.nonNull(environment) && StringUtils.isNotEmpty(environment.getConfig())) {
                taskInstance.setEnvironmentConfig(environment.getConfig());
            }
        }

        if (taskInstance.getSubmitTime() == null) {
            taskInstance.setSubmitTime(new Date());
        }
        if (taskInstance.getFirstSubmitTime() == null) {
            taskInstance.setFirstSubmitTime(taskInstance.getSubmitTime());
        }

        taskInstance.setTaskExecuteType(taskDefinition.getTaskExecuteType());
        taskInstance.setExecutorId(taskExecuteStartMessage.getExecutorId());
        taskInstance.setExecutorName(taskExecuteStartMessage.getExecutorName());

        return taskInstance;
    }

    /**
     * get TaskExecutionContext
     *
     * @param taskInstance taskInstance
     * @return TaskExecutionContext
     */
    protected TaskExecutionContext getTaskExecutionContext(TaskInstance taskInstance) {
        int userId = taskDefinition == null ? 0 : taskDefinition.getUserId();
        String tenantCode = processService.getTenantForProcess(taskExecuteStartMessage.getTenantCode(), userId);

        // verify tenant is null
        if (StringUtils.isBlank(tenantCode)) {
            log.error("tenant not exists,task instance id : {}", taskInstance.getId());
            return null;
        }

        // taskInstance.setResources(getResourceFullNames(taskInstance));

        TaskChannel taskChannel = taskPluginManager.getTaskChannel(taskInstance.getTaskType());
        ResourceParametersHelper resources = taskChannel.getResources(taskInstance.getTaskParams());

        AbstractParameters baseParam = taskPluginManager.getParameters(
                ParametersNode.builder()
                        .taskType(taskInstance.getTaskType())
                        .taskParams(taskInstance.getTaskParams())
                        .build());
        Map<String, Property> propertyMap = paramParsingPreparation(taskInstance, baseParam);
        TaskExecutionContext taskExecutionContext = TaskExecutionContextBuilder.get()
                .buildWorkflowInstanceHost(masterConfig.getMasterAddress())
                .buildTaskInstanceRelatedInfo(taskInstance)
                .buildTaskDefinitionRelatedInfo(taskDefinition)
                .buildResourceParametersInfo(resources)
                .buildBusinessParamsMap(new HashMap<>())
                .buildParamInfo(propertyMap)
                .create();

        taskExecutionContext.setTenantCode(tenantCode);
        taskExecutionContext.setProjectCode(processDefinition.getProjectCode());
        taskExecutionContext.setProcessDefineCode(processDefinition.getCode());
        taskExecutionContext.setProcessDefineVersion(processDefinition.getVersion());
        taskExecutionContext.setLogPath(LogUtils.getTaskInstanceLogFullPath(taskExecutionContext));
        // process instance id default 0
        taskExecutionContext.setProcessInstanceId(0);
        taskExecutionContext.setResources(taskInstance.getResources());
        taskExecutionContextFactory.setDataQualityTaskExecutionContext(taskExecutionContext, taskInstance, tenantCode);
        taskExecutionContextFactory.setK8sTaskRelatedInfo(taskExecutionContext, taskInstance);

        log.info("## taskExecutionContext is:{}", JSONUtils.toJsonString(taskExecutionContext));
        return taskExecutionContext;
    }

    /**
     * get resource map key is full name and value is tenantCode
     */
    protected Map<String, String> getResourceFullNames(TaskInstance taskInstance) {
        Map<String, String> resourcesMap = new HashMap<>();
        AbstractParameters baseParam = taskPluginManager.getParameters(ParametersNode.builder()
                .taskType(taskInstance.getTaskType()).taskParams(taskInstance.getTaskParams()).build());
        log.info("## AbstractParameters is:{}", JSONUtils.toJsonString(baseParam));
        if (baseParam != null) {
            List<ResourceInfo> projectResourceFiles = baseParam.getResourceFilesList();
            if (CollectionUtils.isNotEmpty(projectResourceFiles)) {

                // filter the resources that the resource id equals 0
                Set<ResourceInfo> oldVersionResources =
                        projectResourceFiles.stream().filter(t -> t.getId() == null).collect(Collectors.toSet());
                if (CollectionUtils.isNotEmpty(oldVersionResources)) {
                    oldVersionResources.forEach(t -> resourcesMap.put(t.getRes(),
                            processService.queryTenantCodeByResName(t.getRes(), ResourceType.FILE)));
                }

                // get the resource id in order to get the resource names in batch
                Stream<Integer> resourceIdStream = projectResourceFiles.stream().map(ResourceInfo::getId);
                Set<Integer> resourceIdsSet = resourceIdStream.collect(Collectors.toSet());

                if (CollectionUtils.isNotEmpty(resourceIdsSet)) {
                    Integer[] resourceIds = resourceIdsSet.toArray(new Integer[resourceIdsSet.size()]);

                    List<Resource> resources = processService.listResourceByIds(resourceIds);
                    resources.forEach(t -> resourcesMap.put(t.getFullName(),
                            processService.queryTenantCodeByResName(t.getFullName(), ResourceType.FILE)));
                }
            }
        }

        return resourcesMap;
    }

    protected boolean handleTaskEvent(TaskEvent taskEvent) throws StateEventHandleException, StateEventHandleError {
        measureTaskState(taskEvent);

        if (taskInstance.getState() == null) {
            throw new StateEventHandleError("Task state event handle error due to task state is null");
        }
        if (taskEvent.getState().equals(TaskExecutionStatus.KILL)) {
            taskInstance.setEndTime(taskEvent.getEndTime());
        } else {
            taskInstance.setStartTime(taskEvent.getStartTime());
        }
        taskInstance.setHost(taskEvent.getWorkerAddress());
        taskInstance.setLogPath(taskEvent.getLogPath());
        taskInstance.setExecutePath(taskEvent.getExecutePath());
        taskInstance.setPid(taskEvent.getProcessId());
        taskInstance.setHost(taskEvent.getWorkerAddress());
        taskInstance.setAppLink(taskEvent.getAppIds());
        taskInstance.setState(taskEvent.getState());
        taskInstance.setEndTime(taskEvent.getEndTime());
        taskInstance.setVarPool(taskEvent.getVarPool());
        processService.changeOutParam(taskInstance);
        taskInstanceDao.updateById(taskInstance);

        // send ack
        // sendAckToWorker(taskEvent);

        if (taskEvent.getState().equals(TaskExecutionStatus.KILL)) {
            streamTaskInstanceExecCacheManager.removeByTaskInstanceId(taskInstance.getId());
            log.info("The stream task instance is finish, taskInstanceId:{}, state:{}", taskInstance.getId(),
                    taskEvent.getState());
        }

        return true;
    }

    private void measureTaskState(TaskEvent taskEvent) {
        if (taskEvent == null || taskEvent.getState() == null) {
            // the event is broken
            log.warn("The task event is broken..., taskEvent: {}", taskEvent);
            return;
        }
        if (taskEvent.getState().isFinished()) {
            TaskMetrics.incTaskInstanceByState("finish");
        }
        switch (taskEvent.getState()) {
            case KILL:
                TaskMetrics.incTaskInstanceByState("stop");
                break;
            case SUCCESS:
                TaskMetrics.incTaskInstanceByState("success");
                break;
            case FAILURE:
                TaskMetrics.incTaskInstanceByState("fail");
                break;
            default:
                break;
        }
    }

    public Map<String, Property> paramParsingPreparation(@NonNull TaskInstance taskInstance,
                                                         @NonNull AbstractParameters parameters) {
        // assign value to definedParams here
        Map<String, String> globalParamsMap = taskExecuteStartMessage.getStartParams();
        Map<String, Property> globalParams = ParameterUtils.getUserDefParamsMap(globalParamsMap);

        // combining local and global parameters
        Map<String, Property> localParams = parameters.getInputLocalParametersMap();

        // stream pass params
        parameters.setVarPool(taskInstance.getVarPool());
        Map<String, Property> varParams = parameters.getVarPoolMap();

        if (globalParams.isEmpty() && localParams.isEmpty() && varParams.isEmpty()) {
            return null;
        }

        if (varParams.size() != 0) {
            globalParams.putAll(varParams);
        }
        if (localParams.size() != 0) {
            globalParams.putAll(localParams);
        }

        return globalParams;
    }

    private void sendAckToWorker(TaskEvent taskEvent) {
        // If event handle success, send ack to worker to otherwise the worker will retry this event
        TaskExecuteRunningMessageAck taskExecuteRunningMessageAck =
                new TaskExecuteRunningMessageAck(
                        true,
                        taskEvent.getTaskInstanceId(),
                        masterConfig.getMasterAddress(),
                        taskEvent.getWorkerAddress(),
                        System.currentTimeMillis());
        taskEvent.getChannel().writeAndFlush(taskExecuteRunningMessageAck.convert2Command());
    }

    private enum TaskRunnableStatus {
        CREATED, STARTED,
        ;
    }

    protected void initializeTask() {
        log.info("Begin to initialize task");

        long taskStartTime = System.currentTimeMillis();
        taskExecutionContext.setStartTime(taskStartTime);
        log.info("Set task startTime: {}", taskStartTime);

        String taskAppId = String.format("%s_%s", taskExecutionContext.getProcessInstanceId(),
                taskExecutionContext.getTaskInstanceId());
        taskExecutionContext.setTaskAppId(taskAppId);
        log.info("Set task appId: {}", taskAppId);

        log.info("End initialize task {}", JSONUtils.toPrettyJsonString(taskExecutionContext));
    }

    protected void beforeExecute() {
        taskExecutionContext.setCurrentExecutionStatus(TaskExecutionStatus.RUNNING_EXECUTION);

        try {
            // local execute path
            String execLocalPath = FileUtils.getProcessExecDir(
                    taskExecutionContext.getTenantCode(),
                    taskExecutionContext.getProjectCode(),
                    taskExecutionContext.getProcessDefineCode(),
                    taskExecutionContext.getProcessDefineVersion(),
                    taskExecutionContext.getProcessInstanceId(),
                    taskExecutionContext.getTaskInstanceId());
            log.info("## execLocalPath: {}", execLocalPath);

            taskExecutionContext.setExecutePath(execLocalPath);
            taskExecutionContext.setAppInfoPath(FileUtils.getAppInfoPath(execLocalPath));

            Path executePath = Paths.get(taskExecutionContext.getExecutePath());
            FileUtils.createDirectoryIfNotPresent(executePath);

            if (OSUtils.isSudoEnable()) {
                FileUtils.setFileOwner(executePath, taskExecutionContext.getTenantCode());
            }
        } catch (Throwable ex) {
            throw new TaskException("Cannot create process execute dir", ex);
        }
        log.info("WorkflowInstanceExecDir: {} check successfully", taskExecutionContext.getExecutePath());

        TaskUtils.downloadResourcesIfNeeded(storageOperate, taskExecutionContext);
        log.info("Download resources: {} successfully", taskExecutionContext.getResources());

        TaskUtils.downloadUpstreamFiles(taskExecutionContext, storageOperate);
        log.info("Download upstream files: {} successfully",
                TaskUtils.getFileLocalParams(taskExecutionContext, Direct.IN));

        task = Optional.ofNullable(taskPluginManager.getTaskChannelMap().get(taskExecutionContext.getTaskType()))
                .map(taskChannel -> taskChannel.createTask(taskExecutionContext))
                .orElseThrow(() -> new TaskPluginException(taskExecutionContext.getTaskType()
                        + " task plugin not found, please check the task type is correct."));
        log.info("Task plugin instance: {} create successfully", taskExecutionContext.getTaskType());

        // todo: remove the init method, this should initialize in constructor method
        task.init();
        log.info("Success initialized task plugin instance successfully");

        task.getParameters().setVarPool(taskExecutionContext.getVarPool());
        log.info("Set taskVarPool: {} successfully", taskExecutionContext.getVarPool());

    }

    protected void afterExecute() throws TaskException {
        if (task == null) {
            throw new TaskException("The current task instance is null");
        }

        TaskEvent taskEvent = TaskEvent.builder()
                .taskInstanceId(taskExecutionContext.getTaskInstanceId())
                .state(TaskExecutionStatus.RUNNING_EXECUTION)
                .startTime(DateUtils.getCurrentDate())
                .executePath(taskExecutionContext.getExecutePath())
                .logPath(taskExecutionContext.getLogPath())
                .event(TaskEventType.RUNNING)
                .workerAddress(taskExecutionContext.getWorkflowInstanceHost())
                .appIds(task.getAppIds())
                .processId(task.getProcessId())
                .build();
        addTaskEvent(taskEvent);
        taskExecutionContext.setAppIds(task.getAppIds());
        // streamTaskInstanceExecCacheManager.removeByTaskInstanceId(taskExecutionContext.getTaskInstanceId());
        MasterTaskExecutionContextHolder.putTaskExecutionContext(taskExecutionContext);
        log.info("submit task success！");

    }

    public void executeTask(TaskCallBack taskCallBack) throws TaskException {
        if (task == null) {
            throw new IllegalArgumentException("The task plugin instance is not initialized");
        }
        task.handle(taskCallBack);
    }

    public void savePoint() throws TaskException {
        assert task != null;
        log.info("## Start cancel flink stream task, taskApps is : {}", task.getAppIds());


        if (task instanceof StreamTask) {
            ((StreamTask) task).savePoint();
        }

        log.info("## Flink stream task {} is savepoint success", task.getAppIds());
    }

    public void cancelTask() throws TaskException {
        assert task != null;
        log.info("## Start cancel flink stream task, taskApps is : {}", task.getAppIds());

        task.cancel();
        addTaskEvent(TaskExecutionStatus.KILL);

        log.info("## Flink stream task {} is canceled success", task.getAppIds());
    }

    private void addTaskEvent(TaskExecutionStatus status) {
        addTaskEvent(TaskEvent.builder()
                .taskInstanceId(taskExecutionContext.getTaskInstanceId())
                .state(status)
                .startTime(DateUtils.getCurrentDate())
                .endTime(DateUtils.getCurrentDate())
                .executePath(taskExecutionContext.getExecutePath())
                .logPath(taskExecutionContext.getLogPath())
                .event(TaskEventType.RESULT)
                .workerAddress(taskExecutionContext.getWorkflowInstanceHost())
                .appIds(task.getAppIds())
                .processId(task.getProcessId())
                .build());
    }
}

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

package org.apache.dolphinscheduler.server.worker.processor;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.sift.SiftingAppender;
import com.github.rholder.retry.RetryException;
import io.netty.channel.Channel;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.*;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.command.TaskExecuteAckCommand;
import org.apache.dolphinscheduler.remote.command.TaskExecuteRequestCommand;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.remote.utils.JsonSerializer;
import org.apache.dolphinscheduler.server.entity.TaskExecutionContext;
import org.apache.dolphinscheduler.server.log.TaskLogDiscriminator;
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;
import org.apache.dolphinscheduler.server.worker.runner.TaskExecuteThread;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 *  worker request processor
 */
public class TaskExecuteProcessor implements NettyRequestProcessor {

    private final Logger logger = LoggerFactory.getLogger(TaskExecuteProcessor.class);


    /**
     *  thread executor service
     */
//    private final ExecutorService workerExecService;

    /**
     * shell thread executor service
     */
    private final ExecutorService shellExecService;

    /**
     * proceduer thread executor service
     */
    private final ExecutorService proceduerExecService;

    /**
     * sql thread executor service
     */
    private final ExecutorService sqlThreadExecService;

    /**
     * spark thread executor service
     */
    private final ExecutorService sparkThreadExecutorService;

    /**
     * flink thread executor service
     */
    private final ExecutorService flinkThreadExecutorService;

    /**
     * mr thread exceutor service
     */
    private final ExecutorService mrThreadExecutorService;

    /**
     * python thread executor service
     */
    private final ExecutorService pythonThreadExecutorService;

    /**
     * http thread executor service
     */
    private final ExecutorService httpThreadExecutorService;

    /**
     * datax thread executor service
     */
    private final ExecutorService dataxExecutorService;

    /**
     * sqoop thread executor service
     */
    private final ExecutorService sqoopThreadExecutor;

    /**
     *  worker config
     */
    private final WorkerConfig workerConfig;

    /**
     *  task callback service
     */
    private final TaskCallbackService taskCallbackService;

    public TaskExecuteProcessor(){
        this.taskCallbackService = SpringApplicationContext.getBean(TaskCallbackService.class);
        this.workerConfig = SpringApplicationContext.getBean(WorkerConfig.class);
//        this.workerExecService = ThreadUtils.newDaemonFixedThreadExecutor("Worker-Execute-Thread", workerConfig.getWorkerExecThreads());
        this.shellExecService = ThreadUtils.newDaemonFixedThreadExecutor("Shell-Execute-Thread", workerConfig.getShellExecThreads());
        this.proceduerExecService = ThreadUtils.newDaemonFixedThreadExecutor("Proceduer-Execute-Thread", workerConfig.getProdecuerExecThreads());
        this.sqlThreadExecService = ThreadUtils.newDaemonFixedThreadExecutor("Sql-Execute-Thread", workerConfig.getSqlExecThreads());
        this.sparkThreadExecutorService = ThreadUtils.newDaemonFixedThreadExecutor("Spark-Execute-Thread", workerConfig.getSparkExecThreads());
        this.flinkThreadExecutorService = ThreadUtils.newDaemonFixedThreadExecutor("Flink-Execute-Thread", workerConfig.getFlinkExecThreads());
        this.mrThreadExecutorService = ThreadUtils.newDaemonFixedThreadExecutor("Mr-Execute-Thread", workerConfig.getMrExecThreads());
        this.pythonThreadExecutorService = ThreadUtils.newDaemonFixedThreadExecutor("Python-Execute-Thread", workerConfig.getPythonExecThreads());
        this.httpThreadExecutorService = ThreadUtils.newDaemonFixedThreadExecutor("Http-Execute-Thread", workerConfig.getHttpExecThreads());
        this.dataxExecutorService = ThreadUtils.newDaemonFixedThreadExecutor("Datax-Execute-Thread", workerConfig.getDataXExceThreads());
        this.sqoopThreadExecutor = ThreadUtils.newDaemonFixedThreadExecutor("Sqoop-Execute-Thread", workerConfig.getSqoopExecThreads());
    }

    @Override
    public void process(Channel channel, Command command) {
        Preconditions.checkArgument(CommandType.TASK_EXECUTE_REQUEST == command.getType(),
                String.format("invalid command type : %s", command.getType()));

        TaskExecuteRequestCommand taskRequestCommand = JsonSerializer.deserialize(
                command.getBody(), TaskExecuteRequestCommand.class);

        logger.info("received command : {}", taskRequestCommand);

        String contextJson = taskRequestCommand.getTaskExecutionContext();

        TaskExecutionContext taskExecutionContext = JSONUtils.parseObject(contextJson, TaskExecutionContext.class);
        taskExecutionContext.setHost(OSUtils.getHost() + ":" + workerConfig.getListenPort());

        // local execute path
        String execLocalPath = getExecLocalPath(taskExecutionContext);
        logger.info("task instance  local execute path : {} ", execLocalPath);

        try {
            FileUtils.createWorkDirAndUserIfAbsent(execLocalPath, taskExecutionContext.getTenantCode());
        } catch (Exception ex){
            logger.error(String.format("create execLocalPath : %s", execLocalPath), ex);
        }
        taskCallbackService.addRemoteChannel(taskExecutionContext.getTaskInstanceId(),
                new NettyRemoteChannel(channel, command.getOpaque()));

        // tell master that task is in executing
        final Command ackCommand = buildAckCommand(taskExecutionContext).convert2Command();
        
        try {
            RetryerUtils.retryCall(() -> {
                taskCallbackService.sendAck(taskExecutionContext.getTaskInstanceId(),ackCommand);
                return Boolean.TRUE;
            });

            TaskExecuteThread taskExecuteThread = new TaskExecuteThread(taskExecutionContext, taskCallbackService);
            // submit task
            switch (EnumUtils.getEnum(TaskType.class, taskExecutionContext.getTaskType())) {
                case SHELL:
                    shellExecService.submit(taskExecuteThread);
                    break;
                case PROCEDURE:
                    proceduerExecService.submit(taskExecuteThread);
                    break;
                case SQL:
                    sqlThreadExecService.submit(taskExecuteThread);
                    break;
                case MR:
                    mrThreadExecutorService.submit(taskExecuteThread);
                    break;
                case SPARK:
                    sparkThreadExecutorService.submit(taskExecuteThread);
                    break;
                case FLINK:
                    flinkThreadExecutorService.submit(taskExecuteThread);
                    break;
                case PYTHON:
                    pythonThreadExecutorService.submit(taskExecuteThread);
                    break;
                case HTTP:
                    httpThreadExecutorService.submit(taskExecuteThread);
                    break;
                case DATAX:
                    dataxExecutorService.submit(taskExecuteThread);
                    break;
                case SQOOP:
                    sqoopThreadExecutor.submit(taskExecuteThread);
                    break;
                default:
                    logger.error("unsupport task type: {}", taskExecutionContext.getTaskType());
                    throw new IllegalArgumentException("not support task type");
            }
            // submit task
//            workerExecService.submit(new TaskExecuteThread(taskExecutionContext, taskCallbackService));
        } catch (ExecutionException | RetryException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * get task log path
     * @return log path
     */
    private String getTaskLogPath(TaskExecutionContext taskExecutionContext) {
        String baseLog = ((TaskLogDiscriminator) ((SiftingAppender) ((LoggerContext) LoggerFactory.getILoggerFactory())
                .getLogger("ROOT")
                .getAppender("TASKLOGFILE"))
                .getDiscriminator()).getLogBase();
        if (baseLog.startsWith(Constants.SINGLE_SLASH)){
            return baseLog + Constants.SINGLE_SLASH +
                    taskExecutionContext.getProcessDefineId() + Constants.SINGLE_SLASH  +
                    taskExecutionContext.getProcessInstanceId() + Constants.SINGLE_SLASH  +
                    taskExecutionContext.getTaskInstanceId() + ".log";
        }
        return System.getProperty("user.dir") + Constants.SINGLE_SLASH +
                baseLog +  Constants.SINGLE_SLASH +
                taskExecutionContext.getProcessDefineId() + Constants.SINGLE_SLASH  +
                taskExecutionContext.getProcessInstanceId() + Constants.SINGLE_SLASH  +
                taskExecutionContext.getTaskInstanceId() + ".log";
    }

    /**
     * build ack command
     * @param taskExecutionContext taskExecutionContext
     * @return TaskExecuteAckCommand
     */
    private TaskExecuteAckCommand buildAckCommand(TaskExecutionContext taskExecutionContext) {
        TaskExecuteAckCommand ackCommand = new TaskExecuteAckCommand();
        ackCommand.setTaskInstanceId(taskExecutionContext.getTaskInstanceId());
        ackCommand.setStatus(ExecutionStatus.RUNNING_EXEUTION.getCode());
        ackCommand.setLogPath(getTaskLogPath(taskExecutionContext));
        ackCommand.setHost(taskExecutionContext.getHost());
        ackCommand.setStartTime(new Date());
        if(taskExecutionContext.getTaskType().equals(TaskType.SQL.name()) || taskExecutionContext.getTaskType().equals(TaskType.PROCEDURE.name())){
            ackCommand.setExecutePath(null);
        }else{
            ackCommand.setExecutePath(taskExecutionContext.getExecutePath());
        }
        taskExecutionContext.setLogPath(ackCommand.getLogPath());
        return ackCommand;
    }

    /**
     * get execute local path
     * @param taskExecutionContext taskExecutionContext
     * @return execute local path
     */
    private String getExecLocalPath(TaskExecutionContext taskExecutionContext){
        return FileUtils.getProcessExecDir(taskExecutionContext.getProjectId(),
                taskExecutionContext.getProcessDefineId(),
                taskExecutionContext.getProcessInstanceId(),
                taskExecutionContext.getTaskInstanceId());
    }
}

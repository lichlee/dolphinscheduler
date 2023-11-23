package org.apache.dolphinscheduler.server.master.processor;

import io.netty.channel.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.remote.command.Message;
import org.apache.dolphinscheduler.remote.command.MessageType;
import org.apache.dolphinscheduler.remote.command.task.TaskSavePointRequest;
import org.apache.dolphinscheduler.remote.processor.MasterRpcProcessor;
import org.apache.dolphinscheduler.server.master.cache.StreamTaskInstanceExecCacheManager;
import org.apache.dolphinscheduler.server.master.runner.StreamTaskExecuteRunnable;
import org.apache.dolphinscheduler.server.master.runner.execute.MasterTaskExecutionContextHolder;
import org.springframework.stereotype.Component;

/**
 * @Classname MasterTaskSavePointProcessor
 * @Description Master task save point processor
 * @Date 2023/11/21 14:33
 * @Created by liz010
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MasterTaskSavePointProcessor implements MasterRpcProcessor {

    private final StreamTaskInstanceExecCacheManager streamTaskInstanceExecCacheManager;

    @Override
    public void process(Channel channel, Message message) {
        TaskSavePointRequest taskSavePointRequest = JSONUtils.parseObject(message.getBody(), TaskSavePointRequest.class);
        log.info("## Master receive task savepoint request: {}", taskSavePointRequest);

        int taskInstanceId = taskSavePointRequest.getTaskInstanceId();
        try (LogUtils.MDCAutoClosableContext mdcAutoClosableContext = LogUtils.setTaskInstanceIdMDC(taskInstanceId)) {
            TaskExecutionContext taskExecutionContext =
                    MasterTaskExecutionContextHolder.getTaskExecutionContext(taskInstanceId);
            if (taskExecutionContext == null) {
                log.error("Cannot find the TaskExecutionContext, this task may already been killed");
                return;
            }

            StreamTaskExecuteRunnable streamTaskExecuteRunnable =
                    streamTaskInstanceExecCacheManager.getByTaskInstanceId(taskInstanceId);
            if (streamTaskExecuteRunnable == null) {
                log.error("Cannot find the MasterTaskExecuteRunnable, this task may already been killed");
                return;
            }

            try {
                streamTaskExecuteRunnable.savePoint();
            } catch (TaskException e) {
                log.error("Cancel streamTaskExecuteRunnable failed ", e);
            }
        }
    }

    @Override
    public MessageType getCommandType() {
        return MessageType.TASK_SAVEPOINT_REQUEST;
    }
}

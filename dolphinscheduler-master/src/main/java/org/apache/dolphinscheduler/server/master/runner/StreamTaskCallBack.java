package org.apache.dolphinscheduler.server.master.runner;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.common.enums.TaskEventType;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.model.ApplicationInfo;
import org.apache.dolphinscheduler.server.master.processor.queue.TaskEvent;

/**
 * @Classname StreamTaskCallback
 * @Description 流任务后处理
 * @Date 2023/11/13 15:57
 * @Created by liz010
 */
@Slf4j
@Builder
public class StreamTaskCallBack implements TaskCallBack {
    private final TaskExecutionContext taskExecutionContext;

    private final StreamTaskExecuteRunnable streamTaskExecuteRunnable;

    public StreamTaskCallBack(TaskExecutionContext taskExecutionContext, StreamTaskExecuteRunnable streamTaskExecuteRunnable) {
        this.taskExecutionContext = taskExecutionContext;
        this.streamTaskExecuteRunnable = streamTaskExecuteRunnable;
    }

    @Override
    public void updateRemoteApplicationInfo(int taskInstanceId, ApplicationInfo applicationInfo) {
        TaskEvent taskEvent = TaskEvent.builder()
                .taskInstanceId(taskInstanceId)
                .state(TaskExecutionStatus.RUNNING_EXECUTION)
                .startTime(DateUtils.getCurrentDate())
                .executePath(taskExecutionContext.getExecutePath())
                .logPath(taskExecutionContext.getLogPath())
                .appIds(applicationInfo.getAppIds())
                .event(TaskEventType.RUNNING)
                .build();
        streamTaskExecuteRunnable.addTaskEvent(taskEvent);
        log.info("## updateRemoteApplicationInfo, taskInstanceId : {}", taskInstanceId);
        log.info("## updateRemoteApplicationInfo, ApplicationInfo : {}", applicationInfo.getAppIds());
    }

    @Override
    public void updateTaskInstanceInfo(int taskInstanceId) {
        TaskEvent taskEvent = TaskEvent.builder()
                .taskInstanceId(taskInstanceId)
                .state(TaskExecutionStatus.SUBMITTED_SUCCESS)
                .startTime(DateUtils.getCurrentDate())
                .executePath(taskExecutionContext.getExecutePath())
                .logPath(taskExecutionContext.getLogPath())
                .event(TaskEventType.RUNNING)
                .build();
        streamTaskExecuteRunnable.addTaskEvent(taskEvent);
        log.info("## updateTaskInstanceInfo method, taskInstanceId : {}", taskInstanceId);
    }
}

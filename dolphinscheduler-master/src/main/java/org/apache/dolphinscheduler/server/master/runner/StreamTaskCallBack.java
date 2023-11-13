package org.apache.dolphinscheduler.server.master.runner;

import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.model.ApplicationInfo;

/**
 * @Classname StreamTaskCallback
 * @Description TODO
 * @Date 2023/11/13 15:57
 * @Created by liz010
 */
@Slf4j
public class StreamTaskCallBack implements TaskCallBack {
    @Override
    public void updateRemoteApplicationInfo(int taskInstanceId, ApplicationInfo applicationInfo) {
        log.info("## updateRemoteApplicationInfo, taskInstanceId : {}", taskInstanceId);
        log.info("## updateRemoteApplicationInfo, ApplicationInfo : {}", applicationInfo.getAppIds());
    }

    @Override
    public void updateTaskInstanceInfo(int taskInstanceId) {
        log.info("## updateTaskInstanceInfo method, taskInstanceId : {}", taskInstanceId);
    }
}

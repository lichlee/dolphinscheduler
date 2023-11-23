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

package org.apache.dolphinscheduler.server.master;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.placeholder.BusinessTimeUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.ParameterUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  user define param
 */
public class ParamsTest {

    private static final Logger logger = LoggerFactory.getLogger(ParamsTest.class);

    @Test
    public void systemParamsTest() throws Exception {
        String command = "${system.biz.date}";

        // start process
        Map<String, String> timeParams = BusinessTimeUtils
                .getBusinessTime(CommandType.START_PROCESS,
                        new Date(), null);

        command = ParameterUtils.convertParameterPlaceholders(command, timeParams);

        logger.info("start process : {}", command);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_MONTH, -5);

        command = "${system.biz.date}";
        // complement data
        timeParams = BusinessTimeUtils
                .getBusinessTime(CommandType.COMPLEMENT_DATA,
                        calendar.getTime(), null);
        command = ParameterUtils.convertParameterPlaceholders(command, timeParams);
        logger.info("complement data : {}", command);

    }

    @Test
    public void paramTest(){
        ObjectNode params = JSONUtils.parseObject("{\"appName\":\"FlinkOracle2HudiCDC\",\"deployMode\":\"cluster\",\"flinkVersion\":\"1.11\",\"initScript\":\"\",\"jobManagerMemory\":\"1G\",\"localParams\":[],\"mainClass\":\"com.cmft.Oracle2Hudi\",\"mainJar\":{\"id\":-1,\"res\":null,\"resourceName\":\"file:/dolphinscheduler/default/resources/FlinkCDCOracle2Hudi-1.0-SNAPSHOT.jar\"},\"others\":\"-yt /opt/hadoopclient/Flink/flink/ssl/\",\"parallelism\":1,\"programType\":\"JAVA\",\"rawScript\":\"\",\"resourceList\":[],\"slot\":1,\"taskManager\":2,\"taskManagerMemory\":\"1G\",\"yarnQueue\":\"\"}");
        if (params.has("appName")) {
            params.put("appName", "123");
            Assertions.assertTrue(true);
        }
        System.out.println(JSONUtils.toJsonString(params));
    }

    @Test
    public void paramTest2(){
        Map<String, String> params = JSONUtils.toMap("{\"k1\":\"{\\\"k11\\\":\\\"v11\\\"}}\"}");
        ObjectNode taskParams = JSONUtils.parseObject("{\"k1\":\"v1\"}");
        taskParams.set("k2",JSONUtils.parseObject("{\"k11\":\"v11\"}"));

        System.out.println(JSONUtils.toJsonString(taskParams));
    }
}

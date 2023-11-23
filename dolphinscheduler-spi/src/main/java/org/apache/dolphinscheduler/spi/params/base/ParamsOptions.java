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

package org.apache.dolphinscheduler.spi.params.base;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The options field in form-create`s json rule
 * Set radio, select, checkbox and other component option options
 */
public class ParamsOptions {

    private String label;

    private Object value;

    private String type;

    private String size;

    /**
     * is can be select
     */
    private boolean disabled;

    public ParamsOptions() {
    }

    public ParamsOptions(String label, Object value, boolean disabled) {
        this.label = label;
        this.value = value;
        this.disabled = disabled;
    }

    public ParamsOptions(String label, Object value, String type, String size, boolean disabled) {
        this.label = label;
        this.value = value;
        this.type = type;
        this.size = size;
        this.disabled = disabled;
    }

    @JsonProperty("label")
    public String getLabel() {
        return label;
    }

    public ParamsOptions setLabel(String label) {
        this.label = label;
        return this;
    }

    @JsonProperty("value")
    public Object getValue() {
        return value;
    }

    public ParamsOptions setValue(Object value) {
        this.value = value;
        return this;
    }

    @JsonProperty("type")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("size")
    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    @JsonProperty("disabled")
    public boolean isDisabled() {
        return disabled;
    }

    public ParamsOptions setDisabled(boolean disabled) {
        this.disabled = disabled;
        return this;
    }
}

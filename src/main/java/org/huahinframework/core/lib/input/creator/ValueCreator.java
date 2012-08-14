/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.huahinframework.core.lib.input.creator;

import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.io.Value;

/**
 * From the input data to create a {@link Value}}
 */
public abstract class ValueCreator {
    protected String[] labels;
    protected boolean formatIgnored;

    /**
     * @param labels label of input data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     */
    public ValueCreator(String[] labels, boolean formatIgnored) {
        this.labels = labels;
        this.formatIgnored = formatIgnored;
    }

    /**
     * From the input data to create a {@link Value}}
     * @param strings
     * @param value
     * @throws DataFormatException
     */
    public void create(String[] strings, Value value)
            throws DataFormatException {
        if (labels.length != strings.length) {
            if (formatIgnored) {
                throw new DataFormatException("input format error: " +
                                              "label.length = " + labels.length +
                                              "input.lenght = " + strings.length);
            }
        }

        valueCreate(strings, value);
    }

    /**
     * To create a specific {@link Value}} from the input data.
     * @param strings input data
     * @param value value
     */
    protected abstract void valueCreate(String[] strings, Value value);
}

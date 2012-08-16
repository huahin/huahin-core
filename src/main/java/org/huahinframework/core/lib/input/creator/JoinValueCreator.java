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

import java.util.Map;

import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.io.Value;

/**
 * This class supports the Join when you create a {@link Value}
 */
public class JoinValueCreator extends ValueCreator {
    private String[] masterLabels;
    private int masterJoinNo;
    private int dataJoinNo;
    private Map<String, String[]> simpleJoinMap;

    /**
     * @param labels label of input data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @param masterLabels label of master data
     * @param masterJoinNo master join column number
     * @param dataJoinNo data join column number
     * @param simpleJoinMap join map
     */
    public JoinValueCreator(String[] labels,
                            boolean formatIgnored,
                            String[] masterLabels,
                            int masterJoinNo,
                            int dataJoinNo,
                            Map<String, String[]> simpleJoinMap) {
        super(labels, formatIgnored);
        this.masterLabels = masterLabels;
        this.masterJoinNo = masterJoinNo;
        this.dataJoinNo = dataJoinNo;
        this.simpleJoinMap = simpleJoinMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void valueCreate(String[] strings, Value value) {
        for (int i = 0; i < strings.length; i++) {
            value.addPrimitiveValue(labels[i], strings[i]);
            if (i == dataJoinNo) {
                String[] masters = simpleJoinMap.get(strings[i]);
                if (masters != null) {
                    for (int j = 0; j < masterLabels.length; j++) {
                        if (j != masterJoinNo) {
                            value.addPrimitiveValue(masterLabels[j], masters[j]);
                        }
                    }
                }
            }
        }
    }
}
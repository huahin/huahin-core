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
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.io.Value;

/**
 * This class supports the Join(regex) when you create a {@link Value}
 */
public class JoinRegexValueCreator extends ValueCreator {
    private String[] masterLabels;
    private int masterJoinNo;
    private int dataJoinNo;
    private Map<Pattern, String[]> simpleJoinMap;

    /**
     * @param labels label of input data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @param separator separator
     * @param regex If true, value is regex.
     * @param masterLabels label of master data
     * @param masterJoinNo master join column number
     * @param dataJoinNo data join column number
     * @param simpleJoinMap join map
     */
    public JoinRegexValueCreator(String[] labels,
                                boolean formatIgnored,
                                String separator,
                                boolean regex,
                                String[] masterLabels,
                                int masterJoinNo,
                                int dataJoinNo,
                                Map<Pattern, String[]> simpleJoinMap) {
        super(labels, formatIgnored, separator, regex);
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
                for (Entry<Pattern, String[]> entry : simpleJoinMap.entrySet()) {
                    Pattern p = entry.getKey();
                    if (p.matcher(strings[i]).matches()) {
                        String[] masters = entry.getValue();
                        for (int j = 0; j < masterLabels.length; j++) {
                            value.addPrimitiveValue(masterLabels[j], masters[j]);
                            if (j != masterJoinNo) {
                                value.addPrimitiveValue(masterLabels[j], masters[j]);
                            }
                        }
                    }
                }
            }
        }
    }
}

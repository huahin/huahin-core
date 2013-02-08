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

import org.apache.hadoop.conf.Configuration;
import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.io.Value;
import org.huahinframework.core.util.StringUtil;

/**
 * This class supports the Join(regex) when you create a {@link Value}
 */
public class JoinRegexValueCreator extends JoinCreator {
    private int dataJoinNo;
    private Map<Pattern, String[]> simpleJoinMap;

    /**
     * @param labels label of input data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @param separator separator
     * @param regex If true, value is regex.
     * @param simpleJoinMap join map
     * @param conf Hadoop Job Configuration
     */
    public JoinRegexValueCreator(String[] labels,
                                boolean formatIgnored,
                                String separator,
                                boolean regex,
                                Map<Pattern, String[]> simpleJoinMap,
                                Configuration conf) {
        super(labels, formatIgnored, separator, regex, conf);
        this.simpleJoinMap = simpleJoinMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void init() {
        dataJoinNo = StringUtil.getMatchNo(labels, conf.get(SimpleJob.JOIN_DATA_COLUMN));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void valueCreate(String[] strings, Value value) {
        for (int i = 0; i < strings.length; i++) {
            value.addPrimitiveValue(labels[i], strings[i]);
        }

        for (Entry<Pattern, String[]> entry : simpleJoinMap.entrySet()) {
            Pattern p = entry.getKey();
            if (p.matcher(strings[dataJoinNo]).matches()) {
                String[] masters = entry.getValue();
                for (int i = 0; i < masterLabels.length; i++) {
                    value.addPrimitiveValue(masterLabels[i], masters[i]);
                }
            }
        }
    }
}

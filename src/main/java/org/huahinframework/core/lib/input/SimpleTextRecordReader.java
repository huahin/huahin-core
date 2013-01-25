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
package org.huahinframework.core.lib.input;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.lib.input.creator.JoinRegexValueCreator;
import org.huahinframework.core.lib.input.creator.JoinValueCreator;
import org.huahinframework.core.lib.input.creator.LabelValueCreator;
import org.huahinframework.core.lib.input.creator.SimpleValueCreator;
import org.huahinframework.core.util.HDFSUtils;
import org.huahinframework.core.util.PathUtils;
import org.huahinframework.core.util.S3Utils;

/**
 * Treats keys as offset in file and value as line.
 */
public class SimpleTextRecordReader extends SimpleRecordReader {
    /**
     * {@inheritDoc}
     */
    @Override
    public void init() throws IOException {
        // make value creator
        String[] labels = conf.getStrings(SimpleJob.LABELS);
        if (labels == null) {
            valueCreator = new SimpleValueCreator(separator, regex);
        } else {
            boolean formatIgnored = conf.getBoolean(SimpleJob.FORMAT_IGNORED, false);
            String masterColumn = conf.get(SimpleJob.SIMPLE_JOIN_MASTER_COLUMN);
            if (masterColumn == null) {
                valueCreator = new LabelValueCreator(labels, formatIgnored, separator, regex);
            } else {
                String masterPath = conf.get(SimpleJob.MASTER_PATH);
                String[] masterLabels = conf.getStrings(SimpleJob.MASTER_LABELS);
                String masterSeparator = conf.get(SimpleJob.MASTER_SEPARATOR);
                boolean joinRegex = conf.getBoolean(SimpleJob.JOIN_REGEX, false);

                PathUtils pathUtils = null;
                if(conf.getBoolean(SimpleJob.ONPREMISE, false)) {
                    pathUtils = new HDFSUtils(conf);
                } else {
                    pathUtils = new S3Utils(conf.get(SimpleJob.AWS_ACCESS_KEY),
                                            conf.get(SimpleJob.AWS_SECRET_KEY));
                }

                int masterJoinNo = getJoinNo(masterLabels, masterColumn);
                int dataJoinNo = getJoinNo(labels, conf.get(SimpleJob.SIMPLE_JOIN_DATA_COLUMN));

                Map<String, String[]> simpleJoinMap = null;
                try {
                    simpleJoinMap =
                            pathUtils.getSimpleMaster(masterLabels, masterJoinNo, masterPath, masterSeparator);
                    if (joinRegex) {
                        Map<Pattern, String[]> simpleJoinRegexMap = new HashMap<Pattern, String[]>();
                        for (Entry<String, String[]> entry : simpleJoinMap.entrySet()) {
                            Pattern p = Pattern.compile(entry.getKey());
                            simpleJoinRegexMap.put(p, entry.getValue());
                        }
                        valueCreator =
                                new JoinRegexValueCreator(labels, formatIgnored, separator, regex, masterLabels,
                                                         masterJoinNo, dataJoinNo, simpleJoinRegexMap);
                    } else {
                        valueCreator = new JoinValueCreator(labels, formatIgnored, separator, regex, masterLabels,
                                                            masterJoinNo, dataJoinNo, simpleJoinMap);
                    }
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * get join column number
     * @param labels label's
     * @param join join column
     * @return join column number
     */
    private int getJoinNo(String[] labels, String join) {
        for (int i = 0; i < labels.length; i++) {
            if (join.equals(labels[i])) {
                return i;
            }
        }
        return -1;
    }
}

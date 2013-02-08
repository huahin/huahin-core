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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.lib.input.creator.JoinRegexValueCreator;
import org.huahinframework.core.lib.input.creator.JoinSomeRegexValueCreator;
import org.huahinframework.core.lib.input.creator.JoinSomeValueCreator;
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
        try {
            PathUtils pathUtils = null;
            if(conf.getBoolean(SimpleJob.ONPREMISE, false)) {
                pathUtils = new HDFSUtils(conf);
            } else {
                pathUtils = new S3Utils(conf.get(SimpleJob.AWS_ACCESS_KEY),
                                        conf.get(SimpleJob.AWS_SECRET_KEY));
            }

            String[] labels = conf.getStrings(SimpleJob.LABELS);
            boolean formatIgnored = conf.getBoolean(SimpleJob.FORMAT_IGNORED, false);
            int type = conf.getInt(SimpleJob.READER_TYPE, -1);

            switch (type) {
            case SimpleJob.SIMPLE_READER:
                valueCreator = new SimpleValueCreator(separator, regex);
                break;
            case SimpleJob.LABELS_READER:
                valueCreator = new LabelValueCreator(labels, formatIgnored, separator, regex);
                break;
            case SimpleJob.SINGLE_COLUMN_JOIN_READER:
                Map<String, String[]> simpleJoinMap = pathUtils.getSimpleMaster(conf);
                if (!conf.getBoolean(SimpleJob.JOIN_REGEX, false)) {
                    valueCreator = new JoinValueCreator(labels, formatIgnored, separator,
                                                        formatIgnored, simpleJoinMap, conf);
                } else {
                    Map<Pattern, String[]> simpleJoinRegexMap = new HashMap<Pattern, String[]>();
                    for (Entry<String, String[]> entry : simpleJoinMap.entrySet()) {
                        Pattern p = Pattern.compile(entry.getKey());
                        simpleJoinRegexMap.put(p, entry.getValue());
                    }
                    valueCreator = new JoinRegexValueCreator(labels, formatIgnored, separator,
                                                             formatIgnored, simpleJoinRegexMap, conf);
                }
                break;
            case SimpleJob.SOME_COLUMN_JOIN_READER:
                Map<List<String>, String[]> simpleColumnsJoinMap = pathUtils.getSimpleColumnsMaster(conf);
                if (!conf.getBoolean(SimpleJob.JOIN_REGEX, false)) {
                    valueCreator = new JoinSomeValueCreator(labels, formatIgnored, separator,
                                                            formatIgnored, simpleColumnsJoinMap, conf);
                } else {
                    Map<List<Pattern>, String[]> simpleColumnsJoinRegexMap = new HashMap<List<Pattern>, String[]>();
                    for (Entry<List<String>, String[]> entry : simpleColumnsJoinMap.entrySet()) {
                        List<String> l = entry.getKey();
                        List<Pattern> p = new ArrayList<Pattern>();
                        for (String s : l) {
                            p.add(Pattern.compile(s));
                        }
                        simpleColumnsJoinRegexMap.put(p, entry.getValue());
                    }
                    valueCreator = new JoinSomeRegexValueCreator(labels, formatIgnored, separator,
                                                                 formatIgnored, simpleColumnsJoinRegexMap, conf);
                }
                break;
            default:
                break;
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}

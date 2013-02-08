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
package org.huahinframework.core.lib.join;

import java.io.IOException;

import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.Summarizer;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.writer.Writer;

/**
 * summarizer for big join.
 */
public class JoinSummarizer extends Summarizer {
    private String[] masterLabels;
    private String[] valueLabels;
    private boolean onlyJoin;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void summarize(Writer writer)
            throws IOException, InterruptedException {
        if (!hasNext()) {
            return;
        }

        Record r = next(writer);
        Record masterRecord = new Record();
        for (String s : masterLabels) {
            masterRecord.addValue(s, r.getValueString(s));
        }

        if (onlyJoin) {
            while (hasNext()) {
                Record record = next(writer);
                Record emitRecord = new Record();
                for (String s : valueLabels) {
                    emitRecord.addGrouping(s, record.getValueString(s));
                }
                for (String s : masterLabels) {
                    emitRecord.addGrouping(s, masterRecord.getValueString(s));
                }
                emitRecord.setValueNothing(true);
                writer.write(emitRecord);
            }
        } else {
            while (hasNext()) {
                Record record = next(writer);
                Record emitRecord = new Record();
                for (int i = 0 ;; i++) {
                    String s = record.getGroupingString("JOIN_KEY" + i);
                    if (s == null) {
                        break;
                    }
                    emitRecord.addGrouping("JOIN_KEY" + i, s);
                }
                for (String s : valueLabels) {
                    emitRecord.addValue(s, record.getValueString(s));
                }
                for (String s : masterLabels) {
                    emitRecord.addValue(s, masterRecord.getValueString(s));
                }
                writer.write(emitRecord);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void summarizerSetup() {
        masterLabels = getStringsParameter(SimpleJob.MASTER_LABELS);
        valueLabels = getStringsParameter(SimpleJob.LABELS);
        onlyJoin = getBooleanParameter(SimpleJob.ONLY_JOIN);
    }
}

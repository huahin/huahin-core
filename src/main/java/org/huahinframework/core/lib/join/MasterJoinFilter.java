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

import org.huahinframework.core.Filter;
import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.writer.Writer;

/**
 * this filter is master fileter for big join.
 */
public class MasterJoinFilter extends Filter {
    private String joinColumn;
    private String[] masterLabels;

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
    public void filter(Record record, Writer writer)
            throws IOException, InterruptedException {
        Record emitRecord = new Record();
        emitRecord.addGrouping("JOIN_KEY", record.getValueString(joinColumn));
        emitRecord.addSort(1, Record.SORT_LOWER, 1);
        for (String s : masterLabels) {
            emitRecord.addValue(s, record.getValueString(s));
        }
        writer.write(emitRecord);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void filterSetup() {
        joinColumn = getStringParameter(SimpleJob.SIMPLE_JOIN_MASTER_COLUMN);
        masterLabels = getStringsParameter(SimpleJob.MASTER_LABELS);
    }
}

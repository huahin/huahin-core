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
 * this filter is value fileter for big join.
 */
public class ValueJoinFilter extends Filter {
    private String[] joinColumn;
    private String[] valueLabels;

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

        for (int i = 0; i < joinColumn.length; i++) {
            emitRecord.addGrouping("JOIN_KEY" + i, record.getValueString(joinColumn[i]));
        }

        emitRecord.addSort(2, Record.SORT_LOWER, 1);
        for (String s : valueLabels) {
            emitRecord.addValue(s, record.getValueString(s));
        }
        writer.write(emitRecord);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void filterSetup() {
        int type = getIntParameter(SimpleJob.READER_TYPE);
        if (type == SimpleJob.SINGLE_COLUMN_JOIN_READER) {
            joinColumn = new String[1];
            joinColumn[0] = getStringParameter(SimpleJob.JOIN_DATA_COLUMN);
        } else if (type == SimpleJob.SOME_COLUMN_JOIN_READER) {
            joinColumn = getStringsParameter(SimpleJob.JOIN_DATA_COLUMN);
        }
        valueLabels = getStringsParameter(SimpleJob.LABELS);
    }
}

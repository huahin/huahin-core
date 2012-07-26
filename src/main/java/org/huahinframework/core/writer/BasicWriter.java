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
package org.huahinframework.core.writer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.huahinframework.core.io.Record;

/**
 * BasicWriter is basic Context#write.
 */
public class BasicWriter implements Writer {
    private Record defaultRecord;

    @SuppressWarnings("rawtypes")
    private TaskInputOutputContext context;

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void write(Record record) throws IOException, InterruptedException {
        if (record.isKeyEmpty()) {
            defaultRecord.getKey().getSort().clear();
            record.setKey(defaultRecord.getKey());
        } else {
            if (record.getKey().isGroupingEmpty() &&
                !record.getKey().isSortEmpty()) {
                record.getKey().setGrouping(defaultRecord.getKey().getGrouping());
            }
        }

        if (record.isValueEmpty()) {
            record.setValue(defaultRecord.getValue());
        }

        context.write(record.isGroupingNothing() ? NullWritable.get() : record.getKey(),
                      record.isValueNothing() ? NullWritable.get() : record.getValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() throws IOException, InterruptedException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Record getDefaultRecord() {
        return defaultRecord;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDefaultRecord(Record defaultRecord) {
        this.defaultRecord = defaultRecord;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public TaskInputOutputContext getContext() {
        return context;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void setContext(TaskInputOutputContext context) {
        this.context = context;
    }
}

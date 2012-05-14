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
package org.huahin.core;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.huahin.core.io.Record;

/**
 * <code>ReducerWriter</code> is writer of {@link Summarizer}.
 */
public class ReducerWriter implements Writer {
    private Record defaultRecord;

    @SuppressWarnings("rawtypes")
    private Context context;

    /**
     * Outputs the Record.
     * <p>If the grouping and value is not specified, input is output as it is.</p>
     * @param record write Record
     * @throws IOException
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    @Override
    public void write(Record record)
            throws IOException, InterruptedException {
        if (record.isKeyEmpty()) {
            record.setKey(defaultRecord.getKey());
        }

        if (record.isValueEmpty()) {
            record.setValue(defaultRecord.getValue());
        }

        context.write(record.isGroupingNothing() ? NullWritable.get() : record.getKey(),
                      record.isValueNothing() ? NullWritable.get() : record.getValue());
    }

    /**
     * @return the defaultRecord
     */
    public Record getDefaultRecord() {
        return defaultRecord;
    }

    /**
     * @param defaultRecord the defaultRecord to set
     */
    public void setDefaultRecord(Record defaultRecord) {
        this.defaultRecord = defaultRecord;
    }

    /**
     * @return the context
     */
    @SuppressWarnings("rawtypes")
    public Context getContext() {
        return context;
    }

    /**
     * @param context the context to set
     */
    @SuppressWarnings("rawtypes")
    public void setContext(Context context) {
        this.context = context;
    }
}

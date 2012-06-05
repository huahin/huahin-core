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

import org.apache.hadoop.mapreduce.Reducer;
import org.huahin.core.io.Key;
import org.huahin.core.io.Value;
import org.huahin.core.io.Record;

/**
 * This class is wrapping the {@link Reducer} class.
 * <code>Summarizer</code> will process a given record from {@link Filter}.
 *
 * <p>The framework first calls {@link #summarizerSetup()}, followed by
 * {@link #init()} and {@link #summarizer(Record, Writer)} for each {@link Record} in the input.</p>
 *
 * <p>The following is an example to do a count of the WORD that was passed from Filter.
 * Does not specify the Grouping in the end(), which is used by default.</p>
 *
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class WordSummarizer extends Summarizer {
 *   private int count;
 *
 *   public void init() {
 *     count = 0;
 *   }
 *
 *   public boolean summarizer(Record record, Writer writer)
 *       throws IOException, InterruptedException {
 *     count++;
 *     return false;
 *   }
 *
 *   public void end(Record record, Writer writer)
 *       throws IOException, InterruptedException {
 *     Record emitRecord = new Record();
 *     emitRecord.addValue("COUNT", count);
 *     writer.write(emitRecord);
 *   }
 *
 *   public void summarizerSetup() {
 *   }
 * }
 * </pre></blockquote></p>
 *
 * @see Record
 * @see Writer
 * @see Summarizer
 */
public abstract class Summarizer extends Reducer<Key, Value, Key, Value> {
    protected Context context;
    private Writer writer = new Writer();

    /**
     * {@inheritDoc}
     */
    public void reduce(Key key, Iterable<Value> values, Context context)
            throws IOException ,InterruptedException {
        writer.setContext(context);
        init();

        for (Value value : values) {
            Record record = new Record(key, value);
            writer.setDefaultRecord(record);
            if (summarizer(record, writer)) {
                break;
            }
        }

        Record v = new Record(key, new Value());
        writer.setDefaultRecord(v);
        end(v, writer);
    }

    /**
     * {@inheritDoc}
     */
    public void setup(Context context)
            throws IOException ,InterruptedException {
        this.context = context;
        summarizerSetup();
    }

    /**
     * Get Job String parameter
     * @param name parameter name
     * @return parameter value
     */
    protected String getStringParameter(String name) {
        return context.getConfiguration().get(name);
    }

    /**
     * Get Job Boolean parameter
     * @param name parameter name
     * @return parameter value
     */
    protected boolean getBooleanParameter(String name) {
        return context.getConfiguration().getBoolean(name, false);
    }

    /**
     * Get Job Integer parameter
     * @param name parameter name
     * @return parameterr value
     */
    protected int getIntParameter(String name) {
        return context.getConfiguration().getInt(name, -1);
    }

    /**
     * Will be called before each summarizer is called.
     */
    public abstract void init();

    /**
     * <code>summarizer</code> will process a given record from {@link Filter}.
     * @param record input record
     * @param writer the output using the writer.
     * @return If you return true, and exit the summarizer.
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract boolean summarizer(Record record, Writer writer) throws IOException ,InterruptedException;

    /**
     * end will be executed after the summarizer has been completed.<br>
     * Run the final result were treated with summarizer. But it is not required.
     * @param record last input record
     * @param writer the output using the writer.
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void end(Record record, Writer writer) throws IOException ,InterruptedException;

    /**
     * Called once at the beginning of the task.
     */
    public abstract void summarizerSetup();
}

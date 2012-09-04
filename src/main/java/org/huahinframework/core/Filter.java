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
package org.huahinframework.core;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Record;
import org.huahinframework.core.io.Value;
import org.huahinframework.core.io.ValueWritable;
import org.huahinframework.core.writer.BasicWriter;
import org.huahinframework.core.writer.CombineWriter;
import org.huahinframework.core.writer.Writer;

/**
 * This class is wrapping the {@link Mapper} class.
 * Inherits the <code>Filter</code> class instead of the {@link Mapper} class.
 *
 * <p>Map a simplified process can use the <code>Filter</code> class instead of the <code>Mapper</code> class.
 * Using the filter method, the {@link Record} input is passed instead of the KEY and VALUE.</p>
 *
 * <p>The framework first calls {@link #filterSetup()}, followed by
 * {@link #init()} and {@link #filter(Record, Writer)} for each {@link Record} in the input.
 * {@link #init()} is called before the {@link #filter(Record, Writer)} is called.</p>
 *
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class WordFilter extends Filter {
 *   public void init() {
 *   }
 *
 *   public void filter(Record record, Writer writer)
 *       throws IOException, InterruptedException {
 *     String text = record.getValueString("TEXT");
 *     String[] strings = StringUtil.split(text, StringUtil.TAB, true);
 *     for (String s : strings) {
 *       Record emitRecord = new Record();
 *       emitRecord.addGrouping("WORD", s);
 *       emitRecord.addValue("NUMBER", 1);
 *       writer.write(emitRecord);
 *     }
 *   }
 *
 *   public void filterSetup() {
 *   }
 * }
 * </pre></blockquote></p>
 *
 * @see Record
 * @see Writer
 * @see Summarizer
 */
public abstract class Filter extends Mapper<Key, Value, Key, Value> {
    protected Context context;
    private Writer writer;

    private String[] inputLabels;

    /**
     * {@inheritDoc}
     */
    @Override
    public void map(Key key, Value value, Context context)
            throws IOException,InterruptedException {
        writer.setContext(context);
        init();

        Record record = new Record();
        if (inputLabels != null) {
            int i = 0;
            for (ValueWritable vw : key.getGrouping()) {
                vw.getLabel().set(inputLabels[i++]);
            }
            for (ValueWritable vw : value.getValues()) {
                vw.getLabel().set(inputLabels[i++]);
            }
        }
        record.setKey(key);
        record.setValue(value);

        writer.setDefaultRecord(record);
        filter(record, writer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setup(Context context)
            throws IOException ,InterruptedException {
        this.context = context;
        inputLabels = context.getConfiguration().getStrings(SimpleJob.BEFORE_SUMMARIZER_OUTPUT_LABELS);
        boolean label =
                context.getConfiguration().getStrings(SimpleJob.FILETER_OUTPUT_LABELS) == null ?
                        true : false;

        try {
            if (context.getCombinerClass() != null) {
                writer = new CombineWriter(context.getCombinerClass(),
                                           getIntParameter(SimpleJob.COMBINE_CACHE));
            } else {
                writer = new BasicWriter(label);
            }
        } catch (Exception e) {
            writer = new BasicWriter(label);
        }
        filterSetup();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        writer.flush();
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
     * Will be called before each filter is called.
     */
    public abstract void init();

    /**
     * <code>filter</code> for each {@link Record} in the input.
     * @param record input record
     * @param writer the output using the writer.
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void filter(Record record, Writer writer) throws IOException ,InterruptedException;

    /**
     * Called once at the beginning of the task.
     */
    public abstract void filterSetup();
}

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Value;
import org.huahinframework.core.lib.partition.SimpleGroupingComparator;
import org.huahinframework.core.lib.partition.SimplePartitioner;
import org.huahinframework.core.lib.partition.SimpleSortComparator;

/**
 * This class is wrapping the {@link Job} class.
 */
public class SimpleJob extends Job {
    public static final String LABELS = "LABELS";
    public static final String SEPARATOR = "SEPARATOR";
    public static final String FORMAT_IGNORED = "FORMAT_IGNORED";
    public static final String COMBINE_CACHE = "COMBINE_CACHE";

    public static final int DEFAULT_COMBAIN_CACHE = 200;

    private boolean natural = false;
    private boolean mapper = false;
    private boolean reducer = false;

    /**
     * @throws IOException
     */
    public SimpleJob() throws IOException {
        super();
        setup();
    }

    /**
     * @param conf
     * @throws IOException
     */
    public SimpleJob(Configuration conf) throws IOException {
        super(conf);
        setup();
    }

    /**
     * @param conf
     * @param jobName
     * @throws IOException
     */
    public SimpleJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
        setup();
    }

    /**
     * @param conf
     * @param jobName
     * @param natural
     * @throws IOException
     */
    public SimpleJob(Configuration conf, String jobName, boolean natural) throws IOException {
        super(conf, jobName);
        if (natural) {
            this.natural = natural;
            super.setMapperClass(Mapper.class);
            super.setReducerClass(Reducer.class);
        } else {
            setup();
        }
    }

    /**
     * Default job settings.
     */
    private void setup() {
        super.setMapperClass(Mapper.class);
        super.setMapOutputKeyClass(Key.class);
        super.setMapOutputValueClass(Value.class);

        super.setPartitionerClass(SimplePartitioner.class);
        super.setGroupingComparatorClass(SimpleGroupingComparator.class);
        super.setSortComparatorClass(SimpleSortComparator.class);

        super.setReducerClass(Reducer.class);
        super.setOutputKeyClass(Key.class);
        super.setOutputValueClass(Value.class);
    }

    /**
     * Job {@link Filter} class setting.
     * @param clazz {@link Filter} class
     * @return this
     */
    public SimpleJob setFilter(Class<? extends Mapper<Key, Value, Key, Value>> clazz) {
        super.setMapperClass(clazz);
        mapper = true;
        return this;
    }

    /**
     * Job {@link Summarizer} class setting.
     * @param clazz {@link Summarizer} class
     * @return this
     */
    public SimpleJob setSummarizer(Class<? extends Reducer<Key, Value, Key, Value>> clazz) {
        return setSummarizer(clazz, false, 0);
    }

    /**
     * Job {@link Summarizer} class setting.
     * @param clazz {@link Summarizer} class
     * @param combine If true is set the combiner in the Summarizer
     * @return this
     */
    public SimpleJob setSummarizer(Class<? extends Reducer<Key, Value, Key, Value>> clazz,
                                   boolean combine) {
        return setSummarizer(clazz, combine, DEFAULT_COMBAIN_CACHE);
    }

    /**
     * Job {@link Summarizer} class setting.
     * @param clazz {@link Summarizer} class
     * @param combine If true is set the combiner in the Summarizer
     * @param cache In-Mapper Combine output cahce number. Default value is 200.
     * @return this
     */
    public SimpleJob setSummarizer(Class<? extends Reducer<Key, Value, Key, Value>> clazz,
                                   boolean combine,
                                   int cache) {
        super.setReducerClass(clazz);
        reducer = true;
        if (combine) {
            setCombiner(clazz, cache);
        }
        return this;
    }

    /**
     * Job Conbiner class setting.
     * @param clazz {@link Summarizer} class
     * @return this
     */
    public SimpleJob setCombiner(Class<? extends Reducer<Key, Value, Key, Value>> clazz)
            throws IllegalStateException {
        return setCombiner(clazz, DEFAULT_COMBAIN_CACHE);
    }

    /**
     * Job Conbiner class setting.
     * @param clazz {@link Summarizer} class
     * @param cache In-Mapper Combine output cahce number. Default value is 200.
     * @return this
     */
    public SimpleJob setCombiner(Class<? extends Reducer<Key, Value, Key, Value>> clazz,
                                 int cache)
            throws IllegalStateException {
        super.setCombinerClass(clazz);
        getConfiguration().setInt(COMBINE_CACHE, cache);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void setMapperClass(Class<? extends Mapper> cls)
            throws IllegalStateException {
        super.setMapperClass(cls);
        mapper = true;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void setReducerClass(Class<? extends Reducer> cls)
            throws IllegalStateException {
        super.setReducerClass(cls);
        reducer = true;
    }

    /**
     * Whether to ignore the format.
     * @param formatIgnored Whether to ignore the format
     */
    public void setFormatIgnored(boolean formatIgnored) {
        getConfiguration().setBoolean(FORMAT_IGNORED, formatIgnored);
    }

    /**
     * Job parameter setting.
     * @param name parameter name
     * @param value {@link String} parameter value
     */
    public void setParameter(String name, String value) {
        conf.set(name, value);
    }

    /**
     * Job parameter setting
     * @param name parameter name
     * @param value boolean parameter value
     */
    public void setParameter(String name, boolean value) {
        conf.setBoolean(name, value);
    }

    /**
     * Job parameter setting.
     * @param name parameter name
     * @param value int parameter value
     */
    public void setParameter(String name, int value) {
        conf.setInt(name, value);
    }

    /**
     * Returns if true, set the natural
     * @return the natural If true, set the natural
     */
    public boolean isNatural() {
        return natural;
    }

    /**
     * Returns if true, set the mapper
     * @return the mapper If true, set the mapper
     */
    public boolean isMapper() {
        return mapper;
    }

    /**
     * Returns if true, set the reducer
     * @return the reducer If true, set the reducer
     */
    public boolean isReducer() {
        return reducer;
    }
}

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
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Value;
import org.huahinframework.core.lib.partition.SimpleGroupingComparator;
import org.huahinframework.core.lib.partition.SimplePartitioner;
import org.huahinframework.core.lib.partition.SimpleSortComparator;
import org.huahinframework.core.util.PathUtils;
import org.huahinframework.core.util.SizeUtils;
import org.huahinframework.core.util.StringUtil;

/**
 * This class is wrapping the {@link Job} class.
 */
public class SimpleJob extends Job {
    public static final String LABELS = "LABELS";
    public static final String FILETER_OUTPUT_LABELS = "FILETER_OUTPUT_LABELS";
    public static final String SUMMARIZER_OUTPUT_LABELS = "SUMMARIZER_OUTPUT_LABELS";
    public static final String BEFORE_SUMMARIZER_OUTPUT_LABELS = "BEFORE_SUMMARIZER_OUTPUT_LABELS";
    public static final String MASTER_LABELS = "MASTER_LABELS";
    public static final String MASTER_PATH = "MASTER_PATH";
    public static final String JOIN_REGEX = "JOIN_REGEX";
    public static final String ONLY_JOIN = "ONLY_JOIN";
    public static final String JOIN_MASTER_COLUMN = "JOIN_MASTER_COLUMN";
    public static final String JOIN_DATA_COLUMN = "JOIN_DATA_COLUMN";
    public static final String SEPARATOR = "SEPARATOR";
    public static final String SEPARATOR_REGEX = "SEPARATOR_REGEX";
    public static final String MASTER_SEPARATOR = "MASTER_SEPARATOR";
    public static final String FORMAT_IGNORED = "FORMAT_IGNORED";
    public static final String COMBINE_CACHE = "COMBINE_CACHE";

    public static final String CLUSTER_TYPE = "CLUSTER_TYPE";
    public static final int CLUSTER_TYPE_ONPREMISE = 0;
    public static final int CLUSTER_TYPE_AWS = 1;
    public static final int CLUSTER_TYPE_LOCAL = 2;

    public static final String READER_TYPE = "READER_TYPE";
    public static final int SIMPLE_READER = 0;
    public static final int LABELS_READER = 1;
    public static final int SINGLE_COLUMN_JOIN_READER = 2;
    public static final int SOME_COLUMN_JOIN_READER = 3;

    public static final String AWS_ACCESS_KEY = "AWS_ACCESS_KEY";
    public static final String AWS_SECRET_KEY = "AWS_SECRET_KEY";

    public static final int DEFAULT_COMBAIN_CACHE = 200;

    private static final int DEFAULT_AUTOJOIN_THRESHOLD = 600;
    private static final int DEFAULT_AUTOSOMEJOIN_THRESHOLD = 900;
    private static final int DEFALUT_CHILD_MEM_SIZE = 200;

    private PathUtils pathUtils;
    private boolean bigJoin;

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
     * Easily supports the Join. To use the setSimpleJoin,
     * you must be a size master data appear in the memory of the task.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterPath master data HDFS path
     * @return this
     */
    public SimpleJob setSimpleJoin(String[] masterLabels, String masterColumn,
                                   String dataColumn, String masterPath) {
        String separator = conf.get(SEPARATOR);
        return setSimpleJoin(masterLabels, masterColumn, dataColumn, masterPath, separator, false);
    }

    /**
     * Easily supports the Join. To use the setSimpleJoin,
     * you must be a size master data appear in the memory of the task.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterPath master data HDFS path
     * @param regex master join is regex;
     * @return this
     */
    public SimpleJob setSimpleJoin(String[] masterLabels, String masterColumn, String dataColumn,
                                   String masterPath, boolean regex) {
        String separator = conf.get(SEPARATOR);
        return setSimpleJoin(masterLabels, masterColumn, dataColumn, masterPath, separator, regex);
    }

    /**
     * Easily supports the Join. To use the setSimpleJoin,
     * you must be a size master data appear in the memory of the task.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterPath master data HDFS path
     * @param separator separator
     * @param regex master join is regex
     * @return this
     */
    public SimpleJob setSimpleJoin(String[] masterLabels, String masterColumn, String dataColumn,
                                   String masterPath, String separator, boolean regex) {
        conf.setInt(READER_TYPE, SINGLE_COLUMN_JOIN_READER);
        conf.setStrings(MASTER_LABELS, masterLabels);
        conf.set(JOIN_MASTER_COLUMN, masterColumn);
        conf.set(JOIN_DATA_COLUMN, dataColumn);
        conf.set(MASTER_PATH, masterPath);
        conf.set(MASTER_SEPARATOR, separator);
        conf.setBoolean(JOIN_REGEX, regex);
        return this;
    }

    /**
     * Easily supports the Join. To use the setSimpleJoin,
     * you must be a size master data appear in the memory of the task.
     * @param masterLabels label of master data
     * @param masterColumns master column's
     * @param dataColumns data column's
     * @param masterPath master data HDFS path
     * @return this
     * @throws DataFormatException
     */
    public SimpleJob setSimpleJoin(String[] masterLabels, String[] masterColumns,
                                   String[] dataColumns, String masterPath) throws DataFormatException {
        String separator = conf.get(SEPARATOR);
        return setSimpleJoin(masterLabels, masterColumns, dataColumns, masterPath, separator, false);
    }

    /**
     * Easily supports the Join. To use the setSimpleJoin,
     * you must be a size master data appear in the memory of the task.
     * @param masterLabels label of master data
     * @param masterColumns master column's
     * @param dataColumns data column's
     * @param masterPath master data HDFS path
     * @param regex master join is regex;
     * @return this
     * @throws DataFormatException
     */
    public SimpleJob setSimpleJoin(String[] masterLabels, String[] masterColumns, String[] dataColumns,
                                   String masterPath, boolean regex) throws DataFormatException {
        String separator = conf.get(SEPARATOR);
        return setSimpleJoin(masterLabels, masterColumns, dataColumns, masterPath, separator, regex);
    }

    /**
     * Easily supports the Join. To use the setSimpleJoin,
     * you must be a size master data appear in the memory of the task.
     * @param masterLabels label of master data
     * @param masterColumns master column's
     * @param dataColumns data column's
     * @param masterPath master data HDFS path
     * @param separator separator
     * @param regex master join is regex
     * @return this
     * @throws DataFormatException
     */
    public SimpleJob setSimpleJoin(String[] masterLabels, String[] masterColumns, String[] dataColumns,
            String masterPath, String separator, boolean regex) throws DataFormatException {
        if (masterColumns.length != dataColumns.length) {
            throw new DataFormatException("masterColumns and dataColumns lenght is miss match.");
        }

        conf.setInt(READER_TYPE, SOME_COLUMN_JOIN_READER);
        conf.setStrings(MASTER_LABELS, masterLabels);
        conf.setStrings(JOIN_MASTER_COLUMN, masterColumns);
        conf.setStrings(JOIN_DATA_COLUMN, dataColumns);
        conf.set(MASTER_PATH, masterPath);
        conf.set(MASTER_SEPARATOR, separator);
        conf.setBoolean(JOIN_REGEX, regex);
        return this;
    }

    /**
     * to join the data that does not fit into memory.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterPath master data HDFS path
     * @return this
     */
    public SimpleJob setBigJoin(String[] masterLabels, String masterColumn,
                                String dataColumn, String masterPath) {
        String separator = conf.get(SEPARATOR);
        setBigJoin(masterLabels, masterColumn, dataColumn, masterPath, separator);
        return this;
    }

    /**
     * to join the data that does not fit into memory.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterPath master data HDFS path
     * @param separator separator
     * @return this
     */
    public SimpleJob setBigJoin(String[] masterLabels, String masterColumn, String dataColumn,
                                String masterPath, String separator) {
        if (natural) {
            throw new RuntimeException("Not supported big join for natural job.");
        }

        bigJoin = true;
        conf.setInt(READER_TYPE, SINGLE_COLUMN_JOIN_READER);
        conf.setStrings(MASTER_LABELS, masterLabels);
        conf.set(JOIN_MASTER_COLUMN, masterColumn);
        conf.set(JOIN_DATA_COLUMN, dataColumn);
        conf.set(MASTER_PATH, masterPath);
        conf.set(MASTER_SEPARATOR, separator);
        return this;
    }

    /**
     * to join the data that does not fit into memory.
     * @param masterLabels label of master data
     * @param masterColumns master column's
     * @param dataColumns data column's
     * @param masterPath master data HDFS path
     * @return this
     * @throws DataFormatException
     */
    public SimpleJob setBigJoin(String[] masterLabels, String[] masterColumns,
                                String[] dataColumns, String masterPath) throws DataFormatException {
        String separator = conf.get(SEPARATOR);
        return setBigJoin(masterLabels, masterColumns, dataColumns, masterPath, separator);
    }

    /**
     * to join the data that does not fit into memory.
     * @param masterLabels label of master data
     * @param masterColumns master column's
     * @param dataColumns data column's
     * @param masterPath master data HDFS path
     * @param separator separator
     * @return this
     * @throws DataFormatException
     */
    public SimpleJob setBigJoin(String[] masterLabels, String[] masterColumns, String[] dataColumns,
                                String masterPath, String separator) throws DataFormatException {
        if (natural) {
            throw new RuntimeException("Not supported big join for natural job.");
        }
        if (masterColumns.length != dataColumns.length) {
            throw new DataFormatException("masterColumns and dataColumns lenght is miss match.");
        }

        bigJoin = true;
        conf.setInt(READER_TYPE, SOME_COLUMN_JOIN_READER);
        conf.setStrings(MASTER_LABELS, masterLabels);
        conf.setStrings(JOIN_MASTER_COLUMN, masterColumns);
        conf.setStrings(JOIN_DATA_COLUMN, dataColumns);
        conf.set(MASTER_PATH, masterPath);
        conf.set(MASTER_SEPARATOR, separator);
        return this;
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterPath master data HDFS path
     * @return this
     * @throws URISyntaxException
     * @throws IOException
     */
    public SimpleJob setJoin(String[] masterLabels, String masterColumn,
                             String dataColumn, String masterPath) throws IOException, URISyntaxException {
        String separator = conf.get(SEPARATOR);
        return setJoin(masterLabels, masterColumn, dataColumn, masterPath, separator, false, DEFAULT_AUTOJOIN_THRESHOLD);
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterPath master data HDFS path
     * @param regex master join is regex;
     * @return this
     * @throws URISyntaxException
     * @throws IOException
     */
    public SimpleJob setJoin(String[] masterLabels, String masterColumn, String dataColumn,
                             String masterPath, boolean regex) throws IOException, URISyntaxException {
        String separator = conf.get(SEPARATOR);
        return setJoin(masterLabels, masterColumn, dataColumn, masterPath, separator, regex, DEFAULT_AUTOJOIN_THRESHOLD);
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumn master column
     * @param dataColumn data column
     * @param masterPath master data HDFS path
     * @param separator separator
     * @param regex master join is regex
     * @param threshold auto join threshold size(MB). Default 600MB.
     * @return this
     * @throws URISyntaxException
     * @throws IOException
     */
    public SimpleJob setJoin(String[] masterLabels, String masterColumn, String dataColumn,
                             String masterPath, String separator, boolean regex, int threshold) throws IOException, URISyntaxException {
        if (regex) {
            return setSimpleJoin(masterLabels, masterColumn, dataColumn,
                                 masterPath, separator, regex);
        }

        String javaOpt = conf.get("mapred.child.java.opts");
        String xmx = StringUtil.getXmx(javaOpt);
        int xmxSize = SizeUtils.xmx2MB(xmx);
        int masterSize = SizeUtils.byte2Mbyte(pathUtils.getFileSize(masterPath));

        int freeSize = xmxSize - DEFALUT_CHILD_MEM_SIZE - masterSize;
        if (freeSize < threshold) {
            return setBigJoin(masterLabels, masterColumn, dataColumn, masterPath, separator);
        }

        return setSimpleJoin(masterLabels, masterColumn, dataColumn,
                             masterPath, separator, regex);
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumns master column's
     * @param dataColumns data column's
     * @param masterPath master data HDFS path
     * @return this
     * @throws URISyntaxException
     * @throws IOException
     */
    public SimpleJob setJoin(String[] masterLabels, String[] masterColumns,
                             String[] dataColumns, String masterPath) throws IOException, URISyntaxException {
        String separator = conf.get(SEPARATOR);
        return setJoin(masterLabels, masterColumns, dataColumns, masterPath, separator, false, DEFAULT_AUTOSOMEJOIN_THRESHOLD);
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumns master column's
     * @param dataColumns data column's
     * @param masterPath master data HDFS path
     * @param regex master join is regex;
     * @return this
     * @throws URISyntaxException
     * @throws IOException
     */
    public SimpleJob setJoin(String[] masterLabels, String[] masterColumns, String[] dataColumns,
                                   String masterPath, boolean regex) throws IOException, URISyntaxException {
        String separator = conf.get(SEPARATOR);
        return setJoin(masterLabels, masterColumns, dataColumns, masterPath, separator, regex, DEFAULT_AUTOSOMEJOIN_THRESHOLD);
    }

    /**
     * This method is to determine automatically join the Simple and Big.
     * @param masterLabels label of master data
     * @param masterColumns master column's
     * @param dataColumns data column's
     * @param masterPath master data HDFS path
     * @param separator separator
     * @param regex master join is regex
     * @param threshold auto join threshold size(MB). Default 900MB.
     * @return this
     * @throws URISyntaxException
     * @throws IOException
     */
    public SimpleJob setJoin(String[] masterLabels, String[] masterColumns, String[] dataColumns,
                             String masterPath, String separator, boolean regex, int threshold) throws IOException, URISyntaxException {
        if (masterColumns.length != dataColumns.length) {
            throw new DataFormatException("masterColumns and dataColumns lenght is miss match.");
        }

        if (regex) {
            return setSimpleJoin(masterLabels, masterColumns, dataColumns,
                                 masterPath, separator, regex);
        }

        String javaOpt = conf.get("mapred.child.java.opts");
        String xmx = StringUtil.getXmx(javaOpt);
        int xmxSize = SizeUtils.xmx2MB(xmx);
        int masterSize = SizeUtils.byte2Mbyte(pathUtils.getFileSize(masterPath));

        int freeSize = xmxSize - DEFALUT_CHILD_MEM_SIZE - masterSize;
        if (freeSize < threshold) {
            return setBigJoin(masterLabels, masterColumns, dataColumns, masterPath, separator);
        }

        return setSimpleJoin(masterLabels, masterColumns, dataColumns,
                             masterPath, separator, regex);
    }

    /**
     * Set to output labels of Filter.
     * (By setting the meta-information, you can expect up the performance.)
     * @param labels labels
     * @return this
     */
    public SimpleJob withFilterOutputLabels(String[] labels) {
        conf.setStrings(FILETER_OUTPUT_LABELS, labels);
        return this;
    }

    /**
     * Returns output labels of Filter
     * @return labels
     */
    public String[] getFilterOutputLabels() {
        return conf.getStrings(FILETER_OUTPUT_LABELS);
    }

    /**
     * Set to output labels Summarizer.
     * (By setting the meta-information, you can expect up the performance.)
     * @param labels labels
     * @return this
     */
    public SimpleJob withSummarizerOutputLabels(String[] labels) {
        conf.setStrings(SUMMARIZER_OUTPUT_LABELS, labels);
        return this;
    }

    /**
     * Returns output labels of Summarizer
     * @return labels labels
     */
    public String[] getSummarizerOutputLabels() {
        return conf.getStrings(SUMMARIZER_OUTPUT_LABELS);
    }

    /**
     * Set the output labels of the last Job Summarizer.
     * @param labels before job labels
     * @return this
     */
    public SimpleJob setBeforeSummarizerOutputLabeles(String[] labels) {
        conf.setStrings(BEFORE_SUMMARIZER_OUTPUT_LABELS, labels);
        return this;
    }

    /**
     * Whether to ignore the format.
     * @param formatIgnored Whether to ignore the format
     */
    public void setFormatIgnored(boolean formatIgnored) {
        conf.setBoolean(FORMAT_IGNORED, formatIgnored);
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
    public void setParameter(String name, String[] value) {
        conf.setStrings(name, value);
    }

    /**
     * Job parameter setting
     * @param name parameter name
     * @param value @link String} array parameter value
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
     * Job parameter setting.
     * @param name parameter name
     * @param value long parameter value
     */
    public void setParameter(String name, long value) {
        conf.setLong(name, value);
    }

    /**
     * Job parameter setting.
     * @param name parameter name
     * @param value float parameter value
     */
    public void setParameter(String name, float value) {
        conf.setFloat(name, value);
    }

    /**
     * Job parameter setting.
     * @param name parameter name
     * @param value Enum parameter value
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void setParameter(String name, Enum value) {
        conf.setEnum(name, value);
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

    /**
     * @return the bigJoin
     */
    public boolean isBigJoin() {
        return bigJoin;
    }

    /**
     * @param pathUtils the pathUtils to set
     */
    public void setPathUtils(PathUtils pathUtils) {
        this.pathUtils = pathUtils;
    }
}

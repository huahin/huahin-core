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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Value;
import org.huahinframework.core.lib.input.MasterTextInputFormat;
import org.huahinframework.core.lib.input.SimpleTextInputFormat;
import org.huahinframework.core.lib.input.ValueTextInputFormat;
import org.huahinframework.core.lib.join.JoinSummarizer;
import org.huahinframework.core.lib.join.MasterJoinFilter;
import org.huahinframework.core.lib.join.ValueJoinFilter;
import org.huahinframework.core.lib.partition.SimpleGroupingComparator;
import org.huahinframework.core.lib.partition.SimplePartitioner;
import org.huahinframework.core.lib.partition.SimpleSortComparator;
import org.huahinframework.core.util.HDFSUtils;
import org.huahinframework.core.util.LocalPathUtils;
import org.huahinframework.core.util.OptionUtil;
import org.huahinframework.core.util.PathUtils;
import org.huahinframework.core.util.S3Utils;
import org.huahinframework.core.util.StringUtil;

/**
 * Job tool class is to set the Job.
 *
 * <p>To configure the Job, you will create a tool that inherit from this class.
 * By default, the intermediate file path creation and deletion is done automatically.</p>
 *
 * <p>If you want to perform more advanced configuration, you can also edit the parameters directly.</p>
 *
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class RankingJobTool extends SimpleJobTool {
 *   protected String setInputPath(String[] args) {
 *     return args[0];
 *   }
 *   protected String setOutputPath(String[] args) {
 *     return args[1];
 *   }
 *   protected void setup() throws Exception {
 *     final String[] labels = new String[] { "USER", "DATE", "REFERER", "URL" };
 *
 *     // Labeled the first job
 *     SimpleJob job1 = addJob(labels, StringUtil.TAB);
 *     job1.setFilter(FirstFilter.class);
 *     job1.setSummaizer(FirstSummarizer.class);
 *
 *     // second job
 *     SimpleJob job2 = addJob();
 *     job2.setSummaizer(SecondSummarizer.class);
 *   }
 * }
 * </pre></blockquote></p>
 *
 * @see SimpleJob
 */
public abstract class SimpleJobTool extends Configured implements Tool {
    private static final String INTERMEDIATE_PATH = "%s-%s-intermediate-%d";

    /**
     * MapReduce job name
     */
    protected String jobName;

    /**
     * Sequencal job chain
     */
    protected SequencalJobChain sequencalJobChain = new SequencalJobChain();

    /**
     * {@link Configuration}
     */
    protected Configuration conf;

    /**
     * {@link OptionUtil}
     */
    protected OptionUtil opt;

    /**
     * input args
     */
    private String[] args;

    /**
     * input path
     */
    protected String input;

    /**
     * output path
     */
    protected String output;

    /**
     * List of intermediate file path
     */
    protected List<String> intermediatePaths = new ArrayList<String>();

    /**
     * Utility to set the path.
     * By default, create an instance in HDFSUtils operations of HDFS.
     * If you are in the Amazon Elastic MapReduce is used to create the instance is S3Utils.
     */
    protected PathUtils pathUtils;

    /**
     * Whether to automatically create the intermediate path multistage MpaReduce.
     */
    protected boolean autoIntermediatePath = true;

    /**
     * Whether you want to delete the intermediate file of multistage MpaReduce.
     */
    protected boolean deleteIntermediatePath = true;

    /**
     * {@inheritDoc}
     */
    @Override
    public int run(String[] args) throws Exception {
        this.opt = new OptionUtil(args);
        this.args = opt.getArgs();

        this.conf = getConf();
        if (opt.isLocalMode()) {
            this.pathUtils = new LocalPathUtils();
        } else {
            this.pathUtils = new HDFSUtils(conf);
        }

        this.jobName = StringUtil.createInternalJobID();

        input = setInputPath(this.args);
        output = setOutputPath(this.args);
        setup();

        String[] beforeSummarizerOutputLabel = null;

        // Make the intermediate path
        if (autoIntermediatePath) {
            SequencalJobChain tempChain = new SequencalJobChain();
            Job lastJob = null;
            String lastIntermediatePath = null;
            for (Job j : sequencalJobChain.getJobs()) {
                if (lastJob == null) {
                    if (j instanceof SimpleJob) {
                        SimpleJob sj = (SimpleJob) j;
                        if (!sj.isNatural()) {
                            SimpleTextInputFormat.setInputPaths(j, input);
                            j.setInputFormatClass(SimpleTextInputFormat.class);

                            if (sj.isBigJoin()) {
                                SimpleJob joinJob = addBigJoinJob(sj);
                                String masterPath = joinJob.getConfiguration().get(SimpleJob.MASTER_PATH);
                                MultipleInputs.addInputPath(joinJob, new Path(input), ValueTextInputFormat.class, ValueJoinFilter.class);
                                MultipleInputs.addInputPath(joinJob, new Path(masterPath), MasterTextInputFormat.class, MasterJoinFilter.class);
                                tempChain.add(joinJob);

                                lastIntermediatePath = String.format(INTERMEDIATE_PATH, output, jobName, sequencalJobChain.getJobs().size());
                                intermediatePaths.add(lastIntermediatePath);

                                SequenceFileOutputFormat.setOutputPath(joinJob, new Path(lastIntermediatePath));
                                joinJob.setOutputFormatClass(SequenceFileOutputFormat.class);
                                lastJob = joinJob;

                                SequenceFileInputFormat.setInputPaths(j, lastIntermediatePath);
                                j.setInputFormatClass(SequenceFileInputFormat.class);
                            }
                        } else {
                            TextInputFormat.setInputPaths(j, input);
                            j.setInputFormatClass(TextInputFormat.class);
                        }
                    }
                } else {
                    SequenceFileInputFormat.setInputPaths(j, lastIntermediatePath);
                    j.setInputFormatClass(SequenceFileInputFormat.class);
                }

                if (j instanceof SimpleJob) {
                    SimpleJob sj = (SimpleJob) j;
                    if (!sj.isNatural()) {
                        if (!sj.isMapper() && !sj.isReducer()) {
                            continue;
                        }

                        if (beforeSummarizerOutputLabel != null) {
                            j.getConfiguration().setStrings(SimpleJob.BEFORE_SUMMARIZER_OUTPUT_LABELS,
                                                            beforeSummarizerOutputLabel);
                        }
                        String[] summarizerOutputLabels = sj.getSummarizerOutputLabels();
                        if (summarizerOutputLabels != null) {
                            beforeSummarizerOutputLabel = summarizerOutputLabels;
                        }
                    }
                }

                lastIntermediatePath = String.format(INTERMEDIATE_PATH, output, jobName, sequencalJobChain.getJobs().size());
                intermediatePaths.add(lastIntermediatePath);

                SequenceFileOutputFormat.setOutputPath(j, new Path(lastIntermediatePath));
                j.setOutputFormatClass(SequenceFileOutputFormat.class);

                tempChain.add(j);
                lastJob = j;
            }

            sequencalJobChain = tempChain;

            for (Job j : sequencalJobChain.getJobs()) {
                if (j instanceof SimpleJob) {
                    SimpleJob jj = (SimpleJob) j;
                    if (!jj.isReducer()) {
                        jj.setNumReduceTasks(0);
                    }
                }
            }

            intermediatePaths.remove(lastIntermediatePath);
            TextOutputFormat.setOutputPath(lastJob, new Path(output));
            lastJob.setOutputFormatClass(TextOutputFormat.class);
        }

        // Running all jobs
        SequencalJobExecuteResults results = sequencalJobChain.runAll();

        // Delete the intermediate data
        if (deleteIntermediatePath) {
            for (String path : intermediatePaths) {
                pathUtils.delete(path);
            }
        }

        return results.isAllJobSuccessful() ? 0 : -1;
    }

    /**
     * Get input args
     * @return input args
     */
    protected String[] getArgs() {
        return this.args;
    }

    /**
     * Set the path from the input parameters.
     * @param args input args
     * @return input path
     */
    protected abstract String setInputPath(String[] args);

    /**
     * Set the path from the output parameters.
     * @param args output args
     * @return output path
     */
    protected abstract String setOutputPath(String[] args);

    /**
     * Sets of Job in this method.
     * @throws Exception
     */
    protected abstract void setup() throws Exception;

    /**
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob() throws IOException {
        return addJob(new SimpleJob(conf, jobName), null, null, false, false);
    }

    /**
     * @param natural If true is natural MapReduce
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob(boolean natural) throws IOException {
        return addJob(new SimpleJob(conf, jobName, natural), null, null, false, false);
    }

    /**
     * @param labels label of input data
     * @param separator separator of data
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob(String[] labels, String separator) throws IOException {
        return addJob(new SimpleJob(conf, jobName), labels, separator, false, false);
    }

    /**
     * @param labels label of input data
     * @param pattern data {@link Pattern}
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob(String[] labels, Pattern pattern) throws IOException {
        return addJob(new SimpleJob(conf, jobName), labels, pattern.pattern(), false, true);
    }

    /**
     * @param separator separator of data
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob(String separator) throws IOException {
        return addJob(new SimpleJob(conf, jobName), null, separator, false, false);
    }

    /**
     * @param pattern data {@link Pattern}
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob(Pattern pattern) throws IOException {
        return addJob(new SimpleJob(conf, jobName), null, pattern.pattern(), false, true);
    }

    /**
     * @param labels label of input data
     * @param separator separator of data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob(String[] labels, String separator, boolean formatIgnored) throws IOException {
        return addJob(new SimpleJob(conf, jobName), labels, separator, formatIgnored, false);
    }

    /**
     * @param labels label of input data
     * @param pattern data {@link Pattern}
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob(String[] labels, Pattern pattern, boolean formatIgnored) throws IOException {
        return addJob(new SimpleJob(conf, jobName), labels, pattern.pattern(), formatIgnored, true);
    }

    /**
     * @param job new {@link SimpleJob}
     * @param labels label of input data
     * @param separator separator of data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @param regex If true, value is regex
     * @return new {@link SimpleJob} class
     * @throws IOException
     */
    protected SimpleJob addJob(SimpleJob job,
                               String[] labels,
                               String separator,
                               boolean formatIgnored,
                               boolean regex) throws IOException {
        setConfiguration(job, labels, separator, formatIgnored, regex);
        sequencalJobChain.add(job);
        // TODO: How to do Reduce task.
//        job.setNumReduceTasks(1);
        return job;
    }

    /**
     * create big join SimpleJob
     * @param job job that big join is set.
     * @return big join {@link SimpleJob}
     * @throws IOException
     */
    private SimpleJob addBigJoinJob(SimpleJob job)
                throws IOException {
        Configuration conf = job.getConfiguration();
        String[] labels = conf.getStrings(SimpleJob.LABELS);
        String separator = conf.get(SimpleJob.SEPARATOR);
        boolean regex = conf.getBoolean(SimpleJob.SEPARATOR_REGEX, false);
        boolean formatIgnored = conf.getBoolean(SimpleJob.FORMAT_IGNORED, false);

        SimpleJob joinJob = new SimpleJob(conf, jobName, true);
        setConfiguration(joinJob, labels, separator, formatIgnored, regex);

        Configuration joinConf = joinJob.getConfiguration();
        joinConf.setStrings(SimpleJob.MASTER_LABELS, conf.getStrings(SimpleJob.MASTER_LABELS));
        joinConf.set(SimpleJob.SIMPLE_JOIN_MASTER_COLUMN, conf.get(SimpleJob.SIMPLE_JOIN_MASTER_COLUMN));
        joinConf.set(SimpleJob.SIMPLE_JOIN_DATA_COLUMN, conf.get(SimpleJob.SIMPLE_JOIN_DATA_COLUMN));
        joinConf.set(SimpleJob.MASTER_PATH, conf.get(SimpleJob.MASTER_PATH));
        joinConf.set(SimpleJob.MASTER_SEPARATOR, conf.get(SimpleJob.MASTER_SEPARATOR));

        joinJob.setMapOutputKeyClass(Key.class);
        joinJob.setMapOutputValueClass(Value.class);

        joinJob.setPartitionerClass(SimplePartitioner.class);
        joinJob.setGroupingComparatorClass(SimpleGroupingComparator.class);
        joinJob.setSortComparatorClass(SimpleSortComparator.class);

        joinJob.setSummarizer(JoinSummarizer.class);

        if (!job.isMapper() && !job.isReducer()) {
            joinConf.setBoolean(SimpleJob.ONLY_JOIN, true);
            joinJob.setOutputKeyClass(Value.class);
            joinJob.setOutputValueClass(NullWritable.class);
        } else {
            joinJob.setOutputKeyClass(Key.class);
            joinJob.setOutputValueClass(Value.class);
        }

        return joinJob;
    }

    /**
     * setting configure
     * @param job new {@link SimpleJob}
     * @param labels label of input data
     * @param separator separator of data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @param regex If true, value is regex
     */
    private void setConfiguration(SimpleJob job,
                                  String[] labels,
                                  String separator,
                                  boolean formatIgnored,
                                  boolean regex) {
        Configuration conf = job.getConfiguration();
        if (labels != null) {
            conf.setStrings(SimpleJob.LABELS, labels);
        }

        if (separator != null) {
            conf.set(SimpleJob.SEPARATOR, separator);
        }

        if (regex) {
            conf.setBoolean(SimpleJob.SEPARATOR_REGEX, true);
        }

        if (pathUtils instanceof HDFSUtils) {
            conf.setBoolean(SimpleJob.ONPREMISE, true);
        } else if(pathUtils instanceof S3Utils) {
            S3Utils u = (S3Utils) pathUtils;
            conf.set(SimpleJob.AWS_ACCESS_KEY, u.getAccessKey());
            conf.set(SimpleJob.AWS_SECRET_KEY, u.getSecretKey());

            if (sequencalJobChain.isEmpty()) {
                FileInputFormat.setMinInputSplitSize(job, 134217728);
                FileInputFormat.setMaxInputSplitSize(job, 134217728);
            }
        }

        job.setJarByClass(SimpleJobTool.class);
        job.setFormatIgnored(formatIgnored);
    }
}

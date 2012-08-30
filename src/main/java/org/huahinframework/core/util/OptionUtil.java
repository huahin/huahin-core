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
package org.huahinframework.core.util;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Command line option args util.
 */
public class OptionUtil {
    /**
     * local mode option
     */
    public static final String LOCAL_MODE = "l";

    /**
     * thread number option
     */
    public static final String THREAD_NUMBER = "t";

    /**
     * split size option
     */
    public static final String SPLIT_SIZE = "s";

    /**
     * job name option
     */
    public static final String JOB_NAME = "j";

    /**
     * default number of thread number
     */
    public static final int DEFAULT_THREAD_NUMBER = 4;

    /**
     * default number of split size
     */
    public static final int DEFAULT_SPLIT_SIZE = 134217728;

    private CommandLine cli;

    /**
     * make command line options
     * @param args command line args
     * @throws ParseException
     */
    public OptionUtil(String args[])
            throws ParseException {
        Options options = new Options();
        options.addOption(LOCAL_MODE, false, "local mode");
        options.addOption(THREAD_NUMBER, true, "thread number");
        options.addOption(SPLIT_SIZE, true, "split size");
        options.addOption(JOB_NAME, true, "job name");

        CommandLineParser parser = new BasicParser();
        cli = parser.parse(options, args, true);
    }

    /**
     * Returns if true, local mode.
     * @return If true, local mode.
     */
    public boolean isLocalMode() {
        return cli.hasOption(LOCAL_MODE);
    }

    /**
     * If thread number option is true, returns thread number. If false, returns default number.
     * @return If true, local mode.
     */
    public int getThreadNumber() {
        if (cli.hasOption(THREAD_NUMBER)) {
            return Integer.valueOf(cli.getOptionValue(THREAD_NUMBER));
        }

        return DEFAULT_THREAD_NUMBER;
    }

    /**
     * If split size option is true, returns thread number. If false, returns split size.
     * @return If true, local mode.
     */
    public long getSplitSize() {
        if (cli.hasOption(SPLIT_SIZE)) {
            return Long.valueOf(cli.getOptionValue(SPLIT_SIZE));
        }

        return DEFAULT_SPLIT_SIZE;
    }

    /**
     * If split job name is true, returns the job name. If false, returns null.
     * @return if true, job name
     */
    public String getJobName() {
        if (cli.hasOption(JOB_NAME)) {
            return cli.getOptionValue(JOB_NAME);
        }

        return null;
    }

    /**
     * Returns Other args
     * @return other args
     */
    public String[] getArgs() {
        return cli.getArgs();
    }
}

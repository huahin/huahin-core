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
package org.huahinframework.core.lib.input.creator;

import org.apache.hadoop.conf.Configuration;
import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.io.Value;

/**
 * This class supports the Join when you create a {@link Value}
 */
public abstract class JoinCreator extends ValueCreator {
    protected Configuration conf;
    protected String[] masterLabels;
    protected String masterPath;
    protected String masterSeparator;

    /**
     * @param labels label of input data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     * @param separator separator
     * @param regex If true, value is regex.
     * @param conf Hadoop Job Configuration
     */
    public JoinCreator(String[] labels,
                       boolean formatIgnored,
                       String separator,
                       boolean regex,
                       Configuration conf) {
        super(labels, formatIgnored, separator, regex);
        this.conf = conf;
        this.masterPath = conf.get(SimpleJob.MASTER_PATH);
        this.masterLabels = conf.getStrings(SimpleJob.MASTER_LABELS);
        this.masterSeparator = conf.get(SimpleJob.MASTER_SEPARATOR);
        init();
    }

    /**
     * init
     */
    protected abstract void init();
}

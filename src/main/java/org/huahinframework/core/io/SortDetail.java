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
package org.huahinframework.core.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 */
public class SortDetail extends AbstractDetail<SortDetail> {
    private IntWritable sort = new IntWritable();
    private IntWritable sortPriority = new IntWritable();

    @SuppressWarnings("rawtypes")
    private WritableComparable key;

    /**
     * Default constractor
     */
    public SortDetail() {
    }

    /**
     * @param sort If true, set the sort
     * @param sortPriority the sort order
     */
    public SortDetail(int sort, int sortPriority) {
        this.sort.set(sort);
        this.sortPriority.set(sortPriority);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.sort.readFields(in);
        this.sortPriority.readFields(in);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(DataOutput out) throws IOException {
        this.sort.write(out);
        this.sortPriority.write(out);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(SortDetail o) {
        return sortPriority.compareTo(o.sortPriority);
    }

    /**
     * @return the sort
     */
    public int getSort() {
        return sort.get();
    }

    /**
     * @return the sortPriority
     */
    public int getSortPriority() {
        return sortPriority.get();
    }

    /**
     * @return the key
     */
    @SuppressWarnings("rawtypes")
    public WritableComparable getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    @SuppressWarnings("rawtypes")
    public void setKey(WritableComparable key) {
        this.key = key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return sort + "\t" + sortPriority;
    }
}

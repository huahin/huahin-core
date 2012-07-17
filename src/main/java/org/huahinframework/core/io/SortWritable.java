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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.huahinframework.core.util.ObjectUtil;
import org.huahinframework.core.util.StringUtil;

/**
 * This class is Writable for secondary sort.
 */
public class SortWritable implements WritableComparable<SortWritable> {
    private IntWritable sort = new IntWritable();
    private IntWritable sortPriority = new IntWritable();
    private Writable value;

    public SortWritable() {
    }

    public SortWritable(int sort, int sortPriority, Writable value) {
        this.sort.set(sort);
        this.sortPriority.set(sortPriority);
        this.value = value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            sort.readFields(in);
            sortPriority.readFields(in);
            value = (Writable) ObjectUtil.getClass(in.readByte()).newInstance();
            value.readFields(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(DataOutput out) throws IOException {
        sort.write(out);
        sortPriority.write(out);
        out.writeByte(ObjectUtil.getId(value.getClass()));
        value.write(out);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public int compareTo(SortWritable o) {
        WritableComparable a = (WritableComparable) this.value;
        WritableComparable b = (WritableComparable) o.value;
        return a.compareTo(b);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return sort.toString() + StringUtil.TAB +
               sortPriority.toString() + StringUtil.TAB +
               value.toString();
    }

    /**
     * @return the sort
     */
    public IntWritable getSort() {
        return sort;
    }

    /**
     * @param sort the sort to set
     */
    public void setSort(IntWritable sort) {
        this.sort = sort;
    }

    /**
     * @return the sortPriority
     */
    public IntWritable getSortPriority() {
        return sortPriority;
    }

    /**
     * @param sortPriority the sortPriority to set
     */
    public void setSortPriority(IntWritable sortPriority) {
        this.sortPriority = sortPriority;
    }

    /**
     * @return the value
     */
    public Writable getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(Writable value) {
        this.value = value;
    }
}

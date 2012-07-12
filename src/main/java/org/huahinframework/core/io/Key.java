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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.huahinframework.core.util.HadoopObject;
import org.huahinframework.core.util.ObjectUtil;

/**
 * This class is to set the Key of Hadoop.
 */
public class Key extends BasicWritable implements WritableComparable<Key> {
    private List<SortWritable> sorts = new ArrayList<SortWritable>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            super.readFields(in);
            sorts.clear();
            int entries = in.readInt();
            for (int i = 0; i < entries; i++) {
                SortWritable sw = new SortWritable();
                sw.readFields(in);
                sorts.add(sw);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(sorts.size());
        for (SortWritable sw : sorts) {
            sw.write(out);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Key key) {
        Text one = (Text) this.identifier();
        Text other = (Text) key.identifier();
        return one.compareTo(other);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Key)) {
            return false;
        }

        Key other = (Key) obj;
        if (this.values.size() != other.values.size()) {
            return false;
        }

        return toString().equals(other.toString());
    }

    /**
     * Returns a value that is used for grouping
     * @return grouping values
     */
    public WritableComparable<?> identifier() {
        return new Text(toString());
    }

    /**
     * Returns the Key for sorting.
     * @return value of sort
     */
    public Map<Integer, SortWritable> sort() {
        Map<Integer, SortWritable> m = new TreeMap<Integer, SortWritable>();
        for (SortWritable sw : sorts) {
            m.put(sw.getSortPriority().get(), sw);
        }

        return m;
    }

    /**
     * set new sort
     * @param smw sort
     */
    public void setSort(List<SortWritable> sorts) {
        this.sorts = sorts;
    }

    /**
     * Returns a value that is used for sort
     * @return sort
     */
    public List<SortWritable> getSort() {
        return sorts;
    }

    /**
     * set new grouping
     * @param smw grouping
     */
    public void setGrouping(List<ValueWritable> values) {
        this.values = values;
    }

    /**
     * Returns a value that is used for grouping
     * @return grouping
     */
    public List<ValueWritable> getGrouping() {
        return values;
    }

    /**
     * Returns if true, grouping is nothing.
     * @return If true, grouping is nothing
     */
    public boolean isGroupingEmpty() {
        return super.isEmpty();
    }

    /**
     * Returns if true, sort is nothing.
     * @return If true, sort is nothing
     */
    public boolean isSortEmpty() {
        return sorts.isEmpty();
    }

    /**
     * Clearing the retention value
     */
    public void clear() {
        super.clear();
        sorts.clear();
    }

    /**
     * @param label value's label
     * @param writable add Hadoop Writable
     */
    public void addHadoopValue(String label, WritableComparable<?> writable) {
        addHadoopValue(label, writable, true, Record.SORT_NON, 0);
    }

    /**
     * @param label value's label
     * @param writable add Hadoop Writable
     * @param sort If true, set the sort
     * @param sortPriority the sort order
     */
    public void addHadoopValue(String label, WritableComparable<?> writable, int sort, int sortPriority) {
        if (writable == null) {
            writable = NullWritable.get();
        }

        if (sort != Record.SORT_NON) {
            sorts.add(new SortWritable(sort, sortPriority, writable));
        }
    }

    /**
     * @param label value's label
     * @param writable add Hadoop Writable
     * @param grouping If true, set the grouping
     * @param sort If true, set the sort
     * @param sortPriority the sort order
     */
    public void addHadoopValue(String label, WritableComparable<?> writable, boolean grouping, int sort, int sortPriority) {
        if (writable == null) {
            writable = NullWritable.get();
        }

        if (grouping) {
            values.add(new ValueWritable(label, writable));
        } else if (sort != Record.SORT_NON) {
            sorts.add(new SortWritable(sort, sortPriority, writable));
        }
    }

    /**
     * @param label value's label
     * @param object add Java primitive object
     */
    public void addPrimitiveValue(String label, Object object) {
        addPrimitiveValue(label, object, true, Record.SORT_NON, 0);
    }

    /**
     * @param label value's label
     * @param object add Java primitive object
     * @param sort If true, set the sort
     * @param sortPriority the sort order
     */
    public void addPrimitiveValue(String label, Object object, int sort, int sortPriority) {
        HadoopObject ho = ObjectUtil.primitive2Hadoop(object);
        if (ho.getObject() instanceof WritableComparable) {
            if (sort != Record.SORT_NON) {
                sorts.add(new SortWritable(sort, sortPriority, ho.getObject()));
            }
            return;
        }

        throw new ClassCastException("object not WritableComparable");
    }

    /**
     * @param label value's label
     * @param object add Java primitive object
     * @param grouping If true, set the grouping
     * @param sort If true, set the sort
     * @param sortPriority the sort order
     */
    public void addPrimitiveValue(String label, Object object, boolean grouping, int sort, int sortPriority) {
        HadoopObject ho = ObjectUtil.primitive2Hadoop(object);
        if (ho.getObject() instanceof WritableComparable) {
            if (grouping) {
                values.add(new ValueWritable(label, ho.getObject()));
            } else if (sort != Record.SORT_NON) {
                sorts.add(new SortWritable(sort, sortPriority, ho.getObject()));
            }
            return;
        }

        throw new ClassCastException("object not WritableComparable");
    }
}

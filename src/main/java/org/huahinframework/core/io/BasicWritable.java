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

import org.apache.hadoop.io.Writable;
import org.huahinframework.core.util.HadoopObject;
import org.huahinframework.core.util.ObjectUtil;
import org.huahinframework.core.util.PrimitiveObject;
import org.huahinframework.core.util.StringUtil;

/**
 * This Writable is Writable class of basic.
 */
public class BasicWritable implements Writable {
    protected List<ValueWritable> values = new ArrayList<ValueWritable>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            values.clear();
            int entries = in.readInt();
            for (int i = 0; i < entries; i++) {
                ValueWritable vw = new ValueWritable();
                vw.readFields(in);
                values.add(vw);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.size());
        for (ValueWritable vw : values) {
            vw.write(out);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * Clearing the retention value
     */
    public void clear() {
        values.clear();
    }

    /**
     * Returns if true, values is nothing.
     * @return If true, values is nothing
     */
    public boolean isEmpty() {
        return values.isEmpty();
    }

    /**
     * Returns the number of elements in this value.
     * @return the number of elements in this value
     */
    public int size() {
        return values.size();
    }

    /**
     * @param label target label
     * @return The {@link Writable} value of the label. If it is not null.
     */
    public Writable getHadoopValue(String label) {
        for (ValueWritable vw : values) {
            if (vw.getLabel().toString().equals(label)) {
                return vw;
            }
        }

        return null;
    }

    /**
     * @param label target label
     * @return The Java premitive value of the label. If it is not null.
     */
    public Object getPrimitiveValue(String label) {
        for (ValueWritable vw : values) {
            if (vw.getLabel().toString().equals(label)) {
                return ObjectUtil.hadoop2Primitive(vw.getValue()).getObject();
            }
        }

        return null;
    }

    /**
     * @param label target label
     * @return The {@link HadoopObject} value of the label. If it is not null.
     */
    public HadoopObject getHadoopObject(String label) {
        for (ValueWritable vw : values) {
            if (vw.getLabel().toString().equals(label)) {
                HadoopObject ho =
                        new HadoopObject(ObjectUtil.hadoop2Primitive(vw.getValue()).getType(),
                                         vw.getValue());
                return ho;
            }
        }

        return null;
    }

    /**
     * @param label target label
     * @return The {@link PrimitiveObject} value of the label. If it is not null.
     */
    public PrimitiveObject getPrimitiveObject(String label) {
        for (ValueWritable vw : values) {
            if (vw.getLabel().toString().equals(label)) {
                return ObjectUtil.hadoop2Primitive(vw.getValue());
            }
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (ValueWritable vw : values) {
            sb.append(vw.getValue().toString()).append(StringUtil.TAB);
        }

        if (sb.length() == 0) {
            return "";
        }

        return sb.substring(0, sb.length() - 1);
    }
}

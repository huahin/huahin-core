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

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.huahinframework.core.util.ObjectUtil;

/**
 * This class is Writable for label and value.
 */
public class ValueWritable implements Writable {
    private Text label = new Text();
    private Writable value;

    /**
     * default constractor
     */
    public ValueWritable() {
        value = new ObjectWritable();
    }

    /**
     * @param label value's label
     * @param value writable value
     */
    public ValueWritable(String label, Writable value) {
        this.label.set(label);
        this.value = value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            label.readFields(in);
            value = ObjectUtil.newInstance(in.readByte());
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
        label.write(out);
        out.writeByte(ObjectUtil.getId(value.getClass()));
        value.write(out);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return value.toString();
    }

    /**
     * @return the label
     */
    public Text getLabel() {
        return label;
    }

    /**
     * @param label the label to set
     */
    public void setLabel(Text label) {
        this.label = label;
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

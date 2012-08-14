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

import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.io.Value;

/**
 * This class creates a {@link Value} in the serial number label
 */
public class SimpleValueCreator extends ValueCreator {
    /**
     * default constractor
     */
    public SimpleValueCreator() {
        this(null, false);
    }

    /**
     * @param labels label of input data
     * @param formatIgnored
     * If true, {@link DataFormatException} will be throw if there is a format error.
     * If false is ignored (default).
     */
    public SimpleValueCreator(String[] labels, boolean formatIgnored) {
        super(labels, formatIgnored);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create(String[] strings, Value value)
            throws DataFormatException {
        for (int i = 0; i < strings.length; i++) {
            value.addPrimitiveValue(String.valueOf(i), strings[i]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void valueCreate(String[] strings, Value value) {
    }
}

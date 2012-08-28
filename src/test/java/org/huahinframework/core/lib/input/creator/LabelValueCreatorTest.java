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

import static org.junit.Assert.assertEquals;

import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.io.Value;
import org.junit.Test;

/**
 *
 */
public class LabelValueCreatorTest {
    @Test
    public void testString() throws DataFormatException {
        String[] labels = { "AAA", "BBB", "CCC", "DDD" };

        ValueCreator valueCreator = new LabelValueCreator(labels, false, "\t", false);
        Value value = new Value();

        String string1 = "a\tb\tc\td";
        valueCreator.create(string1, value);
        assertEquals(value.getPrimitiveValue("AAA"), "a");
        assertEquals(value.getPrimitiveValue("BBB"), "b");
        assertEquals(value.getPrimitiveValue("CCC"), "c");
        assertEquals(value.getPrimitiveValue("DDD"), "d");
        assertEquals(value.getPrimitiveValue("EEE"), null);

        value.clear();
        String string2 = "1\t2\t3\t4";
        valueCreator.create(string2, value);
        assertEquals(value.getPrimitiveValue("AAA"), "1");
        assertEquals(value.getPrimitiveValue("BBB"), "2");
        assertEquals(value.getPrimitiveValue("CCC"), "3");
        assertEquals(value.getPrimitiveValue("DDD"), "4");
        assertEquals(value.getPrimitiveValue("EEE"), null);
    }

    @Test
    public void testRegex() throws DataFormatException {
        String[] labels = { "AAA", "BBB", "CCC", "DDD" };

        ValueCreator valueCreator = new LabelValueCreator(labels, false, "^(.*)\\t(.*)\\t(.*)\\t(.*)$", true);
        Value value = new Value();

        String string1 = "a\tb\tc\td";
        valueCreator.create(string1, value);
        assertEquals(value.getPrimitiveValue("AAA"), "a");
        assertEquals(value.getPrimitiveValue("BBB"), "b");
        assertEquals(value.getPrimitiveValue("CCC"), "c");
        assertEquals(value.getPrimitiveValue("DDD"), "d");
        assertEquals(value.getPrimitiveValue("EEE"), null);

        value.clear();
        String string2 = "1\t2\t3\t4";
        valueCreator.create(string2, value);
        assertEquals(value.getPrimitiveValue("AAA"), "1");
        assertEquals(value.getPrimitiveValue("BBB"), "2");
        assertEquals(value.getPrimitiveValue("CCC"), "3");
        assertEquals(value.getPrimitiveValue("DDD"), "4");
        assertEquals(value.getPrimitiveValue("EEE"), null);
    }
}

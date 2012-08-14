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

import java.util.HashMap;
import java.util.Map;

import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.io.Value;
import org.junit.Test;

/**
 *
 */
public class JoinValueCreatorTest {
    @Test
    public void test() throws DataFormatException {
        String[] labels = { "AAA", "BBB", "CCC", "DDD" };
        String[] masterLabels = { "DDD", "NAME" };

        Map<String, String[]> simpleJoinMap = new HashMap<String, String[]>();
        String[] m1 = { "d", "DdddD" };
        simpleJoinMap.put("d", m1);
        String[] m2 = { "4", "IV" };
        simpleJoinMap.put("4", m2);

        ValueCreator valueCreator =
                new JoinValueCreator(labels, true, masterLabels, 0, 3, simpleJoinMap);
        Value value = new Value();

        String[] strings1 = { "a", "b", "c", "d" };
        valueCreator.create(strings1, value);
        assertEquals(value.getPrimitiveValue("AAA"), "a");
        assertEquals(value.getPrimitiveValue("BBB"), "b");
        assertEquals(value.getPrimitiveValue("CCC"), "c");
        assertEquals(value.getPrimitiveValue("DDD"), "d");
        assertEquals(value.getPrimitiveValue("NAME"), "DdddD");

        value.clear();
        String[] strings2 = { "1", "2", "3", "4" };
        valueCreator.create(strings2, value);
        assertEquals(value.getPrimitiveValue("AAA"), "1");
        assertEquals(value.getPrimitiveValue("BBB"), "2");
        assertEquals(value.getPrimitiveValue("CCC"), "3");
        assertEquals(value.getPrimitiveValue("DDD"), "4");
        assertEquals(value.getPrimitiveValue("NAME"), "IV");
    }
}

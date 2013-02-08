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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.io.Value;
import org.junit.Test;

/**
 *
 */
public class JoinSomeValueCreatorTest {
    @Test
    public void testString() throws DataFormatException {
        String[] labels = { "AAA", "BBB", "CCC", "DDD", "EEE" };
        String[] masterLabels = { "DDD", "EEE", "NAME" };

        Map<List<String>, String[]> simpleJoinMap = new HashMap<List<String>, String[]>();
        String[] mv1 = { "d", "e", "DdddD" };
        simpleJoinMap.put(Arrays.asList("d", "e"), mv1);

        String[] mv2 = { "4", "5", "IV" };
        simpleJoinMap.put(Arrays.asList("4", "5"), mv2);

        Configuration conf = new Configuration();
        conf.setStrings(SimpleJob.MASTER_LABELS, masterLabels);
        String[] join = { "DDD", "EEE" };
        conf.setStrings(SimpleJob.JOIN_DATA_COLUMN, join);
        ValueCreator valueCreator =
                new JoinSomeValueCreator(labels, true, "\t", false, simpleJoinMap, conf);
        Value value = new Value();

        String string1 = "a\tb\tc\td\te";
        valueCreator.create(string1, value);
        assertEquals(value.getPrimitiveValue("AAA"), "a");
        assertEquals(value.getPrimitiveValue("BBB"), "b");
        assertEquals(value.getPrimitiveValue("CCC"), "c");
        assertEquals(value.getPrimitiveValue("DDD"), "d");
        assertEquals(value.getPrimitiveValue("EEE"), "e");
        assertEquals(value.getPrimitiveValue("NAME"), "DdddD");

        value.clear();
        String string2 = "1\t2\t3\t4\t5";
        valueCreator.create(string2, value);
        assertEquals(value.getPrimitiveValue("AAA"), "1");
        assertEquals(value.getPrimitiveValue("BBB"), "2");
        assertEquals(value.getPrimitiveValue("CCC"), "3");
        assertEquals(value.getPrimitiveValue("DDD"), "4");
        assertEquals(value.getPrimitiveValue("EEE"), "5");
        assertEquals(value.getPrimitiveValue("NAME"), "IV");
    }

    @Test
    public void testRegex() throws DataFormatException {
        String[] labels = { "AAA", "BBB", "CCC", "DDD", "EEE" };
        String[] masterLabels = { "DDD", "EEE", "NAME" };

        Map<List<String>, String[]> simpleJoinMap = new HashMap<List<String>, String[]>();
        String[] mv1 = { "d", "e", "DdddD" };
        simpleJoinMap.put(Arrays.asList("d", "e"), mv1);

        String[] mv2 = { "4", "5", "IV" };
        simpleJoinMap.put(Arrays.asList("4", "5"), mv2);

        Configuration conf = new Configuration();
        conf.setStrings(SimpleJob.MASTER_LABELS, masterLabels);
        String[] join = { "DDD", "EEE" };
        conf.setStrings(SimpleJob.JOIN_DATA_COLUMN, join);
        ValueCreator valueCreator =
                new JoinSomeValueCreator(labels, true, "^(.*)\\t(.*)\\t(.*)\\t(.*)\\t(.*)$", true, simpleJoinMap, conf);
        Value value = new Value();

        String string1 = "a\tb\tc\td\te";
        valueCreator.create(string1, value);
        assertEquals(value.getPrimitiveValue("AAA"), "a");
        assertEquals(value.getPrimitiveValue("BBB"), "b");
        assertEquals(value.getPrimitiveValue("CCC"), "c");
        assertEquals(value.getPrimitiveValue("DDD"), "d");
        assertEquals(value.getPrimitiveValue("EEE"), "e");
        assertEquals(value.getPrimitiveValue("NAME"), "DdddD");

        value.clear();
        String string2 = "1\t2\t3\t4\t5";
        valueCreator.create(string2, value);
        assertEquals(value.getPrimitiveValue("AAA"), "1");
        assertEquals(value.getPrimitiveValue("BBB"), "2");
        assertEquals(value.getPrimitiveValue("CCC"), "3");
        assertEquals(value.getPrimitiveValue("DDD"), "4");
        assertEquals(value.getPrimitiveValue("EEE"), "5");
        assertEquals(value.getPrimitiveValue("NAME"), "IV");
    }
}

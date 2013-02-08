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
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.huahinframework.core.DataFormatException;
import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.io.Value;
import org.junit.Test;

/**
 *
 */
public class JoinSomeRegexValueCreatorTest {
    @Test
    public void testString() throws DataFormatException {
        String[] labels = { "AAA", "BBB", "CCC", "DDD", "EEE" };
        String[] masterLabels = { "DDD", "EEE", "NAME" };

        Map<List<Pattern>, String[]> simpleJoinMap = new HashMap<List<Pattern>, String[]>();
        String[] m1 = { "^d.*D$", "^e.*E$", "DdddD" };
        simpleJoinMap.put(Arrays.asList(Pattern.compile("^d.*D$"),
                                        Pattern.compile("^e.*E$")),
                          m1);

        String[] m2 = { "^4.*IV$", "^5.*V$", "IV" };
        simpleJoinMap.put(Arrays.asList(Pattern.compile("^4.*IV$"),
                                        Pattern.compile("^5.*V$")),
                          m2);

        Configuration conf = new Configuration();
        conf.setStrings(SimpleJob.MASTER_LABELS, masterLabels);
        String[] join = { "DDD", "EEE" };
        conf.setStrings(SimpleJob.JOIN_DATA_COLUMN, join);
        ValueCreator valueCreator =
                new JoinSomeRegexValueCreator(labels, true, "\t", false, simpleJoinMap, conf);
        Value value = new Value();

        String string1 = "a\tb\tc\tdaaaaD\teaaaaE";
        valueCreator.create(string1, value);
        assertEquals(value.getPrimitiveValue("AAA"), "a");
        assertEquals(value.getPrimitiveValue("BBB"), "b");
        assertEquals(value.getPrimitiveValue("CCC"), "c");
        assertEquals(value.getPrimitiveValue("DDD"), "daaaaD");
        assertEquals(value.getPrimitiveValue("EEE"), "eaaaaE");
        assertEquals(value.getPrimitiveValue("NAME"), "DdddD");

        value.clear();
        String string2 = "1\t2\t3\t41111IV\t51111V";
        valueCreator.create(string2, value);
        assertEquals(value.getPrimitiveValue("AAA"), "1");
        assertEquals(value.getPrimitiveValue("BBB"), "2");
        assertEquals(value.getPrimitiveValue("CCC"), "3");
        assertEquals(value.getPrimitiveValue("DDD"), "41111IV");
        assertEquals(value.getPrimitiveValue("EEE"), "51111V");
        assertEquals(value.getPrimitiveValue("NAME"), "IV");
    }

    @Test
    public void testRegex() throws DataFormatException {
        String[] labels = { "AAA", "BBB", "CCC", "DDD", "EEE" };
        String[] masterLabels = { "DDD", "EEE", "NAME" };

        Map<List<Pattern>, String[]> simpleJoinMap = new HashMap<List<Pattern>, String[]>();
        String[] m1 = { "^d.*D$", "^e.*E$", "DdddD" };
        simpleJoinMap.put(Arrays.asList(Pattern.compile("^d.*D$"),
                                        Pattern.compile("^e.*E$")),
                          m1);

        String[] m2 = { "^4.*IV$", "^5.*V$", "IV" };
        simpleJoinMap.put(Arrays.asList(Pattern.compile("^4.*IV$"),
                                        Pattern.compile("^5.*V$")),
                          m2);

        Configuration conf = new Configuration();
        conf.setStrings(SimpleJob.MASTER_LABELS, masterLabels);
        String[] join = { "DDD", "EEE" };
        conf.setStrings(SimpleJob.JOIN_DATA_COLUMN, join);
        ValueCreator valueCreator =
                new JoinSomeRegexValueCreator(labels, false, "^(.*)\\t(.*)\\t(.*)\\t(.*)\\t(.*)$", true, simpleJoinMap, conf);
        Value value = new Value();

        String string1 = "a\tb\tc\tdaaaaD\teaaaaE";
        valueCreator.create(string1, value);
        assertEquals(value.getPrimitiveValue("AAA"), "a");
        assertEquals(value.getPrimitiveValue("BBB"), "b");
        assertEquals(value.getPrimitiveValue("CCC"), "c");
        assertEquals(value.getPrimitiveValue("DDD"), "daaaaD");
        assertEquals(value.getPrimitiveValue("EEE"), "eaaaaE");
        assertEquals(value.getPrimitiveValue("NAME"), "DdddD");

        value.clear();
        String string2 = "1\t2\t3\t41111IV\t51111V";
        valueCreator.create(string2, value);
        assertEquals(value.getPrimitiveValue("AAA"), "1");
        assertEquals(value.getPrimitiveValue("BBB"), "2");
        assertEquals(value.getPrimitiveValue("CCC"), "3");
        assertEquals(value.getPrimitiveValue("DDD"), "41111IV");
        assertEquals(value.getPrimitiveValue("EEE"), "51111V");
        assertEquals(value.getPrimitiveValue("NAME"), "IV");
    }
}

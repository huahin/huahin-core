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
package org.huahinframework.core.util;

import static org.junit.Assert.assertEquals;

import org.apache.commons.cli.ParseException;
import org.junit.Test;

/**
 *
 */
public class OptionUtilTest {
    @Test
    public void testLocalModeHit() throws ParseException {
        final String[] args = { "-l", "apacheFormatting", "input", "output", "1,4,6" };

        OptionUtil opt = new OptionUtil(args);
        assertEquals(opt.isLocalMode(), true);
    }

    @Test
    public void testLocalModeNotHit() throws ParseException {
        final String[] args = { "apacheFormatting", "input", "output", "1,4,6" };

        OptionUtil opt = new OptionUtil(args);
        assertEquals(opt.isLocalMode(), false);
    }

    @Test
    public void testThreadModeHit() throws ParseException {
        final String[] args = { "-t", "2", "apacheFormatting", "input", "output", "1,4,6" };

        OptionUtil opt = new OptionUtil(args);
        assertEquals(opt.getThreadNumber(), 2);
    }

    @Test
    public void testThreadModeDefault() throws ParseException {
        final String[] args = { "apacheFormatting", "input", "output", "1,4,6" };

        OptionUtil opt = new OptionUtil(args);
        assertEquals(opt.getThreadNumber(), 4);
    }

    @Test
    public void testSplitSizeHit() throws ParseException {
        final String[] args = { "-s", "2", "apacheFormatting", "input", "output", "1,4,6" };

        OptionUtil opt = new OptionUtil(args);
        assertEquals(opt.getSplitSize(), 2);
    }

    @Test
    public void testSplitSizeDefault() throws ParseException {
        final String[] args = { "apacheFormatting", "input", "output", "1,4,6" };

        OptionUtil opt = new OptionUtil(args);
        assertEquals(opt.getSplitSize(), 134217728);
    }

    @Test
    public void testOtherArgs() throws ParseException {
        final String[] args = { "-l", "-t", "4", "-s", "5", "apacheFormatting", "input", "output", "1,4,6" };
        final String[] reArgs = { "apacheFormatting", "input", "output", "1,4,6" };

        OptionUtil opt = new OptionUtil(args);
        String[] s = opt.getArgs();

        assertEquals(s.length, reArgs.length);

        for (int i = 0; i < reArgs.length; i++) {
            assertEquals(s[i], reArgs[i]);
        }
    }

    @Test
    public void testOtherExceptionArgs() throws ParseException {
        final String[] args = { "-p", "AAAA", "apacheFormatting", "input", "output", "1,4,6" };
        final String[] reArgs = { "-p", "AAAA", "apacheFormatting", "input", "output", "1,4,6" };

        OptionUtil opt = new OptionUtil(args);
        String[] s = opt.getArgs();

        assertEquals(s.length, reArgs.length);

        for (int i = 0; i < reArgs.length; i++) {
            assertEquals(s[i], reArgs[i]);
        }
    }

    @Test
    public void testOtherException2Args() throws ParseException {
        final String[] args = { "-l", "-t", "4", "-s", "5", "-p", "AAAA", "apacheFormatting", "input", "output", "1,4,6" };
        final String[] reArgs = { "-p", "AAAA", "apacheFormatting", "input", "output", "1,4,6" };

        OptionUtil opt = new OptionUtil(args);
        String[] s = opt.getArgs();

        assertEquals(s.length, reArgs.length);

        for (int i = 0; i < reArgs.length; i++) {
            assertEquals(s[i], reArgs[i]);
        }
    }
}

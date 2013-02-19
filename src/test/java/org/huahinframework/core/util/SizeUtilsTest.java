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

import static org.junit.Assert.*;

import org.junit.Test;

/**
 *
 */
public class SizeUtilsTest {
    private int size = -1;
    @Test
    public void testDefaultSize() {
        size = SizeUtils.xmx2MB("");
        assertEquals(size, 200);

        size = SizeUtils.xmx2MB(null);
        assertEquals(size, 200);
    }

    @Test
    public void testByte() {
        size = SizeUtils.xmx2MB("-Xmx1048576");
        assertEquals(size, 1);
    }

    @Test
    public void testKByte() {
        size = SizeUtils.xmx2MB("-Xmx1024k");
        assertEquals(size, 1);

        size = SizeUtils.xmx2MB("-Xmx1024K");
        assertEquals(size, 1);
    }

    @Test
    public void testMByte() {
        size = SizeUtils.xmx2MB("-Xmx1024m");
        assertEquals(size, 1024);

        size = SizeUtils.xmx2MB("-Xmx2048M");
        assertEquals(size, 2048);
    }

    @Test
    public void testGByte() {
        size = SizeUtils.xmx2MB("-Xmx1g");
        assertEquals(size, 1024);

        size = SizeUtils.xmx2MB("-Xmx2G");
        assertEquals(size, 2048);
    }

    @Test
    public void testByte2MByte() {
        size = SizeUtils.byte2Mbyte(1048576);
        assertEquals(size, 1);
    }
}

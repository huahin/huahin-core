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

import java.util.HashMap;
import java.util.Map;

/**
 * Size Utils
 */
public class SizeUtils {
    private static final int K = 2;
    private static final int M = 3;
    private static final int G = 4;

    private static final Map<String, Integer> unitMap = new HashMap<String, Integer>();
    static {
        unitMap.put("k", K);
        unitMap.put("K", K);
        unitMap.put("m", M);
        unitMap.put("M", M);
        unitMap.put("g", G);
        unitMap.put("G", G);
    }

    /**
     * -Xmx to MByte
     * @param xmx
     * @return MByte
     */
    public static int xmx2MB(String xmx) {
        int size = 200;
        if (xmx == null || xmx.isEmpty()) {
            return size;
        }

        String s = xmx.substring(xmx.length() - 1, xmx.length());
        int unit = -1;
        int localSize = -1;
        if (unitMap.get(s) == null) {
            localSize = Integer.valueOf(xmx.substring(4, xmx.length()));
            return localSize / 1024 / 1024;
        } else {
            unit = unitMap.get(s);
            localSize = Integer.valueOf(xmx.substring(4, xmx.length() - 1));
        }

        switch (unit) {
        case K:
            size = localSize / 1024;
            break;
        case M:
            size = localSize;
            break;
        case G:
            size = localSize * 1024;
            break;
        }

        return size;
    }

    /**
     * byte to MByte
     * @param b
     * @return MByte
     */
    public static int byte2Mbyte(long b) {
        return (int)(b / 1024 / 1024);
    }
}

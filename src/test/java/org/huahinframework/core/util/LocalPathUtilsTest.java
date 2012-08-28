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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.Map;

import org.junit.Test;

/**
 *
 */
public class LocalPathUtilsTest {
    private PathUtils utils = new LocalPathUtils();

    @Test
    public void testDelete()
            throws IOException, URISyntaxException {
        File deletePath = new File(System.getProperty("java.io.tmpdir") + "/LocalPathUtils");
        deletePath.mkdir();
        assertEquals(deletePath.exists(), true);

        File.createTempFile("part-r-00000", "", deletePath);
        File.createTempFile("part-r-00001", "", deletePath);
        File.createTempFile("part-r-00002", "", deletePath);
        File.createTempFile("part-r-00003", "", deletePath);
        File.createTempFile("part-r-00004", "", deletePath);
        File.createTempFile("_SUCCESS", "", deletePath);

        utils.delete(deletePath.getPath());
        assertEquals(deletePath.exists(), false);
    }

    @Test
    public void testGetSimpleMaster()
            throws IOException, URISyntaxException {
        final String[] MASTER_LABELS = { "ID", "NAME" };
        final int joinColumnNo = 0;

        File tmpPath = new File(System.getProperty("java.io.tmpdir"));
        File tmp = File.createTempFile("MASTER", "", tmpPath);

        PrintWriter pw = new PrintWriter(tmp);
        pw.println("1\tA");
        pw.println("2\tB");
        pw.println("3\tC");
        pw.close();

        Map<String, String[]> m = utils.getSimpleMaster(MASTER_LABELS, joinColumnNo, tmp.getPath(), "\t");

        assertEquals(m.size(), 3);

        assertEquals(m.get("1").length, 2);
        assertEquals(m.get("1")[0], "1");
        assertEquals(m.get("1")[1], "A");

        assertEquals(m.get("2").length, 2);
        assertEquals(m.get("2")[0], "2");
        assertEquals(m.get("2")[1], "B");

        assertEquals(m.get("3").length, 2);
        assertEquals(m.get("3")[0], "3");
        assertEquals(m.get("3")[1], "C");

        tmp.delete();
        assertEquals(tmp.exists(), false);
    }
}

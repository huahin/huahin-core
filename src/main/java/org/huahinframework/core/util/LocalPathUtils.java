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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class LocalPathUtils implements PathUtils {
    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String path)
            throws IOException, URISyntaxException {
        File deletePath = new File(path);
        for (File f : deletePath.listFiles()) {
            f.delete();
        }
        deletePath.delete();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String[]> getSimpleMaster(String[] masterLabels,
                                                 int joinColumnNo,
                                                 String path,
                                                 String separator)
            throws IOException, URISyntaxException {
        Map<String, String[]> m = new HashMap<String, String[]>();

        File file = new File(path);
        if (!file.exists()) {
            return null;
        }

        BufferedReader br =
                new BufferedReader(new InputStreamReader(new FileInputStream(file)));

        String line;
        while ((line = br.readLine()) != null) {
            String[] strings = StringUtil.split(line, separator, false);
            if (masterLabels.length != strings.length) {
                continue;
            }

            String joinData = strings[joinColumnNo];
            String[] data = new String[strings.length];
            for (int i = 0; i < strings.length; i++) {
                data[i] = strings[i];
            }

            m.put(joinData, data);
        }
        br.close();

        return m;
    }
}

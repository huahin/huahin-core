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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Is a utility for the HDFS.
 */
public class HDFSUtils implements PathUtils {
    private Configuration conf;

    /**
     * @param conf {@link Configuration}
     */
    public HDFSUtils(Configuration conf) {
        this.conf = conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fstatuss = fs.listStatus(new Path(path));
        if (fstatuss != null) {
            for (FileStatus fstatus : fstatuss) {
                fs.delete(fstatus.getPath(), true);
            }
        }
        fs.delete(new Path(path), true);
    }

    /**
     * {@inheritDoc}
     * @throws IOException
     */
    @Override
    public Map<String, String[]> getSimpleMaster(String[] masterLabels,
                                                 int joinColumnNo,
                                                 String path,
                                                 String separator) throws IOException {
        Map<String, String[]> m = new HashMap<String, String[]>();

        FileSystem fs = FileSystem.get(conf);
        FileStatus fstatus = fs.getFileStatus(new Path(path));
        if (fstatus == null) {
            return null;
        }

        BufferedReader br =
                new BufferedReader(new InputStreamReader(fs.open(fstatus.getPath())));

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

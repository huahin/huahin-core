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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Is a utility for the path.
 */
public interface PathUtils {
    /**
     * Removes the specified path.
     * @param path path to be deleted.
     * @throws IOException
     * @throws URISyntaxException
     */
    public void delete(String path) throws IOException, URISyntaxException;

    /**
     * get file size
     * @param path
     * @return file byte size
     * @throws IOException
     * @throws URISyntaxException
     */
    public long getFileSize(String path)  throws IOException, URISyntaxException;;

    /**
     * create simple master data
     * @param conf Hadoop Job Configuration
     * @throws IOException
     */
    public Map<String, String[]> getSimpleMaster(Configuration conf)
                                                         throws IOException, URISyntaxException;

    /**
     * create simple master data
     * @param masterLabels master labels
     * @param joinColumnNo column number for join
     * @param path master path
     * @param separator data separator
     * @return master master data. Dose not exist is null.
     * @throws IOException
     */
    public Map<String, String[]> getSimpleMaster(String[] masterLabels, int joinColumnNo,
                                                 String path, String separator)
                                                         throws IOException, URISyntaxException;

    /**
     * create simple column's master data
     * @param conf Hadoop Job Configuration
     * @throws IOException
     */
    public Map<List<String>, String[]> getSimpleColumnsMaster(Configuration conf)
                                                         throws IOException, URISyntaxException;

    /**
     * create simple column's master data
     * @param masterLabels master labels
     * @param joinColumnNo column number's for join
     * @param path master path
     * @param separator data separator
     * @return master master data. Dose not exist is null.
     * @throws IOException
     */
    public Map<List<String>, String[]> getSimpleColumnsMaster(String[] masterLabels, int[] joinColumnNo,
                                                              String path, String separator)
                                                                  throws IOException, URISyntaxException;
}

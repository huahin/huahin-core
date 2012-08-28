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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Split by a regex String data.
 */
public class RegexSplitter implements Splitter {
    private Pattern pattern;

    /**
     * @param separator separator is regex.
     */
    public RegexSplitter(String separator) {
        this.pattern = Pattern.compile(separator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] split(String str) {
        Matcher matcher = pattern.matcher(str);

        String[] strings = { "" };
        if (matcher.matches()) {
            int groupCount = matcher.groupCount();
            strings = new String[groupCount];
            for (int i = 1; i <= groupCount; i++) {
                strings[i - 1] = matcher.group(i);
            }
        }
        return strings;
    }
}

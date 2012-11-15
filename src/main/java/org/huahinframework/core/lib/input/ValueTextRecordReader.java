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
package org.huahinframework.core.lib.input;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.huahinframework.core.SimpleJob;
import org.huahinframework.core.io.Key;
import org.huahinframework.core.io.Value;
import org.huahinframework.core.lib.input.creator.LabelValueCreator;
import org.huahinframework.core.lib.input.creator.SimpleValueCreator;
import org.huahinframework.core.lib.input.creator.ValueCreator;
import org.huahinframework.core.util.StringUtil;

/**
 * Treats keys as offset in file and value as line.
 */
public class ValueTextRecordReader extends RecordReader<Key, Value> {
    private static final Log LOG = LogFactory.getLog(ValueTextRecordReader.class);

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private Text text = new Text();
    private Key key = null;
    private Value value = null;
    private String separator;

    private ValueCreator valueCreator;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                        Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            in = new LineReader(codec.createInputStream(fileIn), job);
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new LineReader(fileIn, job);
        }

        // skip first line and re-establish "start".
        if (skipFirstLine) {
            start += in.readLine(new Text(), 0,
                        (int)Math.min((long)Integer.MAX_VALUE, end - start));
        }

        Configuration conf = context.getConfiguration();
        this.pos = start;
        this.separator = conf.get(SimpleJob.SEPARATOR, StringUtil.COMMA);
        boolean regex = conf.getBoolean(SimpleJob.SEPARATOR_REGEX, false);

        // make valu creator
        String[] labels = conf.getStrings(SimpleJob.LABELS);
        if (labels == null) {
            valueCreator = new SimpleValueCreator(separator, regex);
        } else {
            boolean formatIgnored = conf.getBoolean(SimpleJob.FORMAT_IGNORED, false);
            valueCreator = new LabelValueCreator(labels, formatIgnored, separator, regex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new Key();
        }
        key.clear();
        key.addPrimitiveValue("KEY", pos);
        if (value == null) {
            value = new Value();
        }
        value.clear();
        int newSize = 0;
        while (pos < end) {
            newSize = in.readLine(text, maxLineLength,
                                  Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                           maxLineLength));
            if (newSize == 0) {
                break;
            }

            valueCreator.create(text.toString(), value);

            pos += newSize;
            if (newSize < maxLineLength) {
                break;
            }

            // line too long. try again
            LOG.info("Skipped line of size " + newSize + " at pos " +
                     (pos - newSize));
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Key getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Value getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}
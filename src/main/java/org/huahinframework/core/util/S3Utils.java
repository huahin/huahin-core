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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Is a utility for the AWS S3.
 */
public class S3Utils implements PathUtils {
    private AmazonS3 s3;
    private String accessKey;
    private String secretKey;

    /**
     * @param accessKey AWS access key
     * @param secretKey AWS secret key
     */
    public S3Utils(String accessKey, String secretKey) {
        s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String path) throws IOException, URISyntaxException {
        URI uri = new URI(path);
        String bucketName = uri.getHost();
        String key = uri.getPath().substring(1, uri.getPath().length());

        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<DeleteObjectsRequest.KeyVersion>();
        String marker = null;
        for (;;) {
            ObjectListing ol =
                s3.listObjects(new ListObjectsRequest().withBucketName(bucketName)
                                                       .withPrefix(key)
                                                       .withMarker(marker));
            for (S3ObjectSummary objectSummary : ol.getObjectSummaries()) {
                keys.add(new DeleteObjectsRequest.KeyVersion(objectSummary.getKey()));
            }

            marker = ol.getNextMarker();
            if (marker == null) {
                break;
            }
        }

        s3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys));
        s3.deleteObject(bucketName, key);
    }

    /**
     * {@inheritDoc}
     * @throws IOException
     * @throws URISyntaxException
     */
    @Override
    public Map<String, String[]> getSimpleMaster(String[] masterLabels,
                                                 String joinColumn,
                                                 String path,
                                                 String separator) throws IOException, URISyntaxException {
        Map<String, String[]> m = new HashMap<String, String[]>();

        URI uri = new URI(path);
        String key = uri.getPath().substring(1);
        S3Object s3Object = s3.getObject(uri.getHost(), key);

        int joinNo = 0;
        for (int i = 0; i < masterLabels.length; i++) {
            if (joinColumn.equals(masterLabels[i])) {
                joinNo = i;
                break;
            }
        }

        BufferedReader br =
                new BufferedReader(new InputStreamReader(s3Object.getObjectContent(), "UTF-8"));

        String line;
        while ((line = br.readLine()) != null) {
            String[] strings = StringUtil.split(line, separator, false);
            if (masterLabels.length != strings.length) {
                continue;
            }

            String joinData = strings[joinNo];
            String[] data = new String[strings.length];
            for (int i = 0; i < strings.length; i++) {
                data[i] = strings[i];
            }

            m.put(joinData, data);
        }
        br.close();

        return m;
    }

    /**
     * @return the accessKey
     */
    public String getAccessKey() {
        return accessKey;
    }

    /**
     * @return the secretKey
     */
    public String getSecretKey() {
        return secretKey;
    }
}

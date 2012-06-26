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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Is a utility for the AWS S3.
 */
public class S3Utils implements PathUtils {
    private AmazonS3 s3;

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
}

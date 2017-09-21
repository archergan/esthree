/*
 * Copyright 2014 Ray Holder
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.rholder.esthree.command;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.github.rholder.esthree.progress.MutableProgressListener;
import com.github.rholder.esthree.progress.TransferProgressWrapper;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;

public class BulkPut implements Callable<Integer> {

    public AmazonS3Client amazonS3Client;
    public String bucket;
    //public String key;
    //public File inputFile;
    public Map<File, String> filesWithKeys;
    public Map<String, String> metadata;
    public boolean sse;

    public MutableProgressListener progressListener;

    public BulkPut(AmazonS3Client amazonS3Client, String bucket, Map<File,String> filesWithKeys, Map<String, String> metadata, boolean sse) {
        this.amazonS3Client = amazonS3Client;
        this.bucket = bucket;
        //this.key = key;
        //this.inputFile = inputFile;
        this.filesWithKeys = filesWithKeys;
        this.metadata = metadata;
        this.sse = sse;
    }

    public BulkPut withProgressListener(MutableProgressListener progressListener) {
        this.progressListener = progressListener;
        return this;
    }

    @Override
    public Integer call() throws Exception {
        TransferManager t = new TransferManager(amazonS3Client);

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setUserMetadata(metadata);
        if(sse) {
            objectMetadata.setSSEAlgorithm(SSEAlgorithm.AES256.getAlgorithm());
        }

        Upload u = null;
        for (Map.Entry<File, String> entry : filesWithKeys.entrySet()){
            u = t.upload(new PutObjectRequest(bucket, entry.getValue(), entry.getKey()).withMetadata(objectMetadata));
        }

        // TODO this listener spews out garbage >100% on a retry, add a test to verify
        if (progressListener != null) {
            progressListener.withTransferProgress(new TransferProgressWrapper(u.getProgress()));
            u.addProgressListener(progressListener);
        }
        try {
            u.waitForCompletion();
        } finally {
            t.shutdownNow();
        }
        return 0;
    }
}
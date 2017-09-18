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

package com.github.rholder.esthree.cli;

import com.github.rholder.esthree.command.Put;
import com.github.rholder.esthree.progress.MutableProgressListener;
import com.github.rholder.esthree.progress.PrintingProgressListener;
import com.github.rholder.esthree.progress.TimeProvider;
import com.github.rholder.esthree.util.S3PathUtils;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;


import static com.google.common.base.Objects.firstNonNull;
import static java.util.Collections.emptyList;

@Command(name = "bulkput", description = "Upload multiple files to S3 in the target bucket")
public class BulkPutCommand extends EsthreeCommand {

    @Option(name = {"-np", "--no-progress"}, description = "Don't print a progress bar")
    public Boolean progress;

    @Option(name = {"-sse", "--server-side-encryption"}, description = "Enable server side encryption with AES256")
    public Boolean sse;

    @Option(name = {"-meta", "--metadata"}, arity = 2, description = "Add additional metadata to an uploaded S3 object, as in --metadata is-potato \"totally a potato\"")
    public List<String> metadata;

    @Arguments(description = "Upload multiple files via a file with one filename per line to S3 with the target bucket and optionally the key, as in \"foo.txt s3://bucket/foo.html\"",
            usage = "[filename containing list of files] [target bucket and key]")
    public List<String> parameters;

    public String bucket;
    //public String key;
    public File outputFilesList;
    public Map<File, String> outputFilesWithKey;
    public MutableProgressListener progressListener;
    public Map<String, String> convertedMetadata;

    @Override
    public void parse() {
        if (help) {
            showUsage(commandMetadata);
            return;
        }

        if (firstNonNull(parameters, emptyList()).size() == 0) {
            showUsage(commandMetadata);
            throw new IllegalArgumentException("No arguments specified");
        }

        // TODO foo s3://bucket  <--- support this?
        if (parameters.size() != 2) {
            output.print("Invalid number of arguments");
            throw new RuntimeException("Invalid number of arguments");
        }
        LineIterator iter;
        try {
            iter = FileUtils.lineIterator(outputFilesList);
        }catch (IOException e){
            System.out.println("IOException with outputFilesList");
            showUsage(commandMetadata);
            return;
        }
        while (iter.hasNext()) {
            File output = FileUtils.getFile(iter.next());
            if (!output.exists() || output.isDirectory()){
                System.out.print("WARNING: File" + output.getName() + "  in input list does not exist or is a directory and was skipped");
            }
            else {
                outputFilesWithKey.put(new File(iter.next()), "");
            }
        }

        String target = parameters.get(1);

        bucket = S3PathUtils.getBucket(target);
        if (bucket == null) {
            output.print("Could not parse bucket name");
            throw new RuntimeException("Could not parse bucket name");
        }

        // by default, always show progress bar
        progress = progress == null;

        // by default, don't enable server side encryption
        sse = sse != null;

        for (Map.Entry<File,String> entry : outputFilesWithKey.entrySet()){
            String key = S3PathUtils.getPrefix(target);
            if (key == null) {
                key = entry.getKey().getName();
            } else if (key.endsWith("/")) {
                // if file ends with "/", also infer name from passed in file
                key = key + entry.getKey().getName();
            }
            entry.setValue(key);

        }
        /*key = S3PathUtils.getPrefix(target);
        // infer name from passed in file if it's not specified in the s3:// String
        if (key == null) {
            key = outputFile.getName();
        } else if (key.endsWith("/")) {
            // if file ends with "/", also infer name from passed in file
            key = key + outputFile.getName();
        }*/

        if (progress) {
            progressListener = new PrintingProgressListener(output, new TimeProvider());
        }

        convertedMetadata = new HashMap<String, String>();
        if (metadata != null) {
            for (int i = 0; i < metadata.size(); i += 2) {
                convertedMetadata.put(metadata.get(i), metadata.get(i + 1));
            }
        }
    }

    @Override
    public void run() {
        if (!help) {
            try {
                for (Map.Entry<File,String> entry : outputFilesWithKey.entrySet()){
                    new Put(amazonS3Client, bucket, entry.getValue(), entry.getKey(), convertedMetadata, sse)
                            .withProgressListener(progressListener)
                            .call();
                }
                /*new Put(amazonS3Client, bucket, key, outputFile, convertedMetadata, sse)
                        .withProgressListener(progressListener)
                        .call();*/
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}

package com.btoddb.chronicle.apps;

/*
 * #%L
 * chronicle
 * %%
 * Copyright (C) 2014 btoddb.com
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * #L%
 */

import com.btoddb.chronicle.plunkers.hdfs.StorableAvroEvent;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 *
 */
public class AvroTools {
    private static final Logger logger = LoggerFactory.getLogger(AvroTools.class);

    // hdfs://dn7dmphnn01.dcloud.starwave.com:9000
    // hdfs://n7cldhnn05.dcloud.starwave.com:9000
    private FileSystem hdfsFs;
    private Configuration hdfsConfig = new Configuration();


    private void echoFile(Path inFile) throws IOException {
        FileContext context = FileContext.getFileContext(hdfsConfig);
        AvroFSInput input = new AvroFSInput(context, inFile);


        ReflectDatumReader<StorableAvroEvent> reader = new ReflectDatumReader<>(StorableAvroEvent.class);
        FileReader<StorableAvroEvent> fileReader = DataFileReader.openReader(input, reader);
        long count = 0;
        try {
            Schema schema = fileReader.getSchema();
            for (StorableAvroEvent event : fileReader) {
                count++;
                System.out.println("event -> " + event.toString());
            }
        }
        finally {
            fileReader.close();
        }

        System.out.println("count = " + count);
    }



    private void testFileAndFix(Path inFile) throws IOException {
        FileContext context = FileContext.getFileContext(hdfsConfig);
        AvroFSInput input = new AvroFSInput(context, inFile);


        ReflectDatumReader<Object> reader = new ReflectDatumReader<>();
        FileReader<Object> fileReader = DataFileReader.openReader(input, reader);

        Path outFile = inFile.suffix(".fixing");
        FSDataOutputStream output = FileSystem.create(outFile.getFileSystem(hdfsConfig), outFile, FsPermission.getDefault());
        DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
        writer.setCodec(CodecFactory.snappyCodec());

        boolean corrupted = false;
        long count = 0;

        try {
            Schema schema = fileReader.getSchema();
            writer.create(schema, output);

            for (;;) {
                try {
                    if (fileReader.hasNext()) {
                        Object obj = fileReader.next();
                        count ++;
                        writer.append(obj);
                    }
                    else {
                        break;
                    }
                }
                catch (AvroRuntimeException e) {
                    corrupted = true;
                    System.out.println("  - file pointer = " + input.tell());
                    if (e.getCause() instanceof EOFException) {
                        System.out.println("  - EOF occurred so we're done : " + e.getMessage());
                        break;
                    }
                    else if (e.getCause() instanceof IOException) {
                        System.out.println("  - will try to 'next' past the error : " + e.getMessage());
                        try {
                            fileReader.next();
                            System.out.println("  - 'next' worked - didn't really expect it to, but great!");
                        }
                        catch (Exception e2) {
                            System.out.println("  - 'next' did not work - will continue on and see what happens : " + e2.getMessage());
                        }
                        continue;
                    }
                    break;
                }
                catch (Exception e) {
                    corrupted = true;
                    System.out.println("  - file pointer = " + input.tell());
                    e.printStackTrace();
                    break;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            System.out.println(("  - processed " + count + " records"));
            if (null != fileReader) {
                try {
                    fileReader.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (null != writer) {
                try {
                    writer.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        if (!corrupted) {
            outFile.getFileSystem(hdfsConfig).delete(outFile, false);
        }
        else {
            outFile.getFileSystem(hdfsConfig).rename(outFile, inFile.suffix(".fixed"));
        }
    }

    private void go(String srcDir) throws URISyntaxException, IOException {
        hdfsFs = FileSystem.get(new URI(srcDir), hdfsConfig);

        System.out.println();
        System.out.println("Processing files from " + srcDir);
        System.out.println();

        logger.debug("Searching for files in {}", srcDir);
        Path path = new Path(srcDir);
        if (!hdfsFs.exists(path)) {
            System.out.println("The path does not exist - cannot continue : " + path.toString());
            return;
        }

        FileStatus[] statuses = hdfsFs.listStatus(path, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String name = path.getName();
                return !name.startsWith("_") && name.endsWith(".avro");
            }
        });

        for (FileStatus fs : statuses) {
            try {
                Path inPath = fs.getPath();
                long fileSize = hdfsFs.getFileStatus(inPath).getLen();
                System.out.println(String.format("Processing file, %s (%d)", inPath.toString(), fileSize));

                testFileAndFix(inPath);
            }
            catch (Exception e) {
                // don't care about the cause, the test should be able to read all files it cares about
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        AvroTools verify = new AvroTools();
        verify.echoFile(new Path("tmp/chronicle/hdfs/btoddb/2014-10-19/file.1413700673342.avro"));
//        verify.go("hdfs://n7cldhnn05.dcloud.starwave.com:9000/data/ESPN-ALERTS-NOTIFICATIONS-PROD/cls/logging/day=2014-08-05");
//        verify.go("/btoddb/projects-disney/avro-defrag-mr/test-avro");
    }

}

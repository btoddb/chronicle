package com.btoddb.chronicle.plunkers;

/*
 * #%L
 * fast-persistent-queue
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

import com.btoddb.chronicle.Config;
import com.btoddb.chronicle.Event;
import com.btoddb.chronicle.plunkers.hdfs.HdfsFile;
import com.btoddb.chronicle.plunkers.hdfs.HdfsFileContext;
import com.btoddb.chronicle.plunkers.hdfs.HdfsFileFactory;
import com.btoddb.chronicle.plunkers.hdfs.HdfsTextFileImpl;
import com.btoddb.chronicle.plunkers.hdfs.HdfsTokenValueProviderImpl;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.integration.junit4.JMockit;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


@RunWith(JMockit.class)
public class HdfsPlunkerImplTest {
    HdfsPlunkerImpl plunker;
    Config config = new Config();
    File baseDir;


    @Before
    public void setup() throws Exception {
        baseDir = new File("tmp/" + UUID.randomUUID().toString());

        plunker = new HdfsPlunkerImpl();
        plunker.setPathPattern(String.format("file://%s/the/${header:customer}/path", baseDir.getPath()));
        plunker.setPermNamePattern("file.avro");
        plunker.setOpenNamePattern("_file.avro.tmp");
    }

    @After
    public void cleanup() {
        try {
            FileUtils.deleteDirectory(baseDir);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateFilePatterns() throws Exception {
        HdfsTokenValueProviderImpl provider = new HdfsTokenValueProviderImpl();
        plunker.init(config);

        plunker.createFilePatterns();

        String prefix = "file://" + baseDir + "/";
        assertThat(plunker.getPathPattern(), is(prefix+"the/${header:customer}/path"));
        assertThat(plunker.getPermNamePattern(), is("file.avro"));
        assertThat(plunker.getOpenNamePattern(), is("_file.avro.tmp"));

        Event event = new Event("the-body").withHeader("customer", "dsp").withHeader("timestamp", "123");
        assertThat(plunker.getKeyTokenizedFilePath().render(event, provider), is(prefix + "the/dsp/path/file.avro"));
        assertThat(plunker.getPermTokenizedFilePath().render(event, provider), is(prefix + "the/dsp/path/file."+provider.render("now")+".avro"));
        assertThat(plunker.getOpenTokenizedFilePath().render(event, provider), is(prefix + "the/dsp/path/_file.avro."+provider.render("now")+".tmp"));
    }

    @Test
    public void testInit(
            @Injectable final ScheduledThreadPoolExecutor closeExec, // don't want other executors affected
            @Injectable final ScheduledThreadPoolExecutor idleExec // don't want other executors affected
    ) throws Exception {
        new Expectations() {{
            idleExec.scheduleWithFixedDelay((Runnable) any, 10000, 10000, TimeUnit.MILLISECONDS); times = 1;
        }};
        plunker.setCloseExec(closeExec);
        plunker.setIdleTimerExec(idleExec);
        plunker.init(config);
    }

    @Test
    public void testShutdown(
            @Injectable final ScheduledThreadPoolExecutor closeExec, // don't want other executors affected
            @Injectable final ScheduledThreadPoolExecutor idleExec // don't want other executors affected
    ) throws Exception {
        new Expectations() {{
            idleExec.scheduleWithFixedDelay((Runnable) any, 10000, 10000, TimeUnit.MILLISECONDS); times = 1;
            idleExec.shutdown(); times = 1;
            closeExec.shutdown(); times = 1;
            closeExec.awaitTermination(plunker.getShutdownWaitTimeout(), TimeUnit.SECONDS); times = 1; result = true;
        }};
        plunker.setCloseExec(closeExec);
        plunker.setIdleTimerExec(idleExec);
        plunker.init(config);
        plunker.shutdown();
    }

    @Test
    public void testInitThenHandleEventThenShutdown(
            @Mocked final HdfsFileFactory fileFactory,
            @Injectable final ScheduledThreadPoolExecutor closeExec, // don't want other executors affected
            @Mocked final HdfsFileContext aContext,
            @Mocked final HdfsTextFileImpl aFile,
            @Mocked final ScheduledFuture<Void> aFuture
    ) throws Exception {
        final List<Event> events = Arrays.asList(
                new Event("the-body").withHeader("msgId", "msg1").withHeader("customer", "customer1"),
                new Event("the-body").withHeader("msgId", "msg2").withHeader("customer", "customer2")
        );

        new NonStrictExpectations() {{
            for (int i=1;i <= 2;i++) {
                HdfsFile hdfsFile = new HdfsTextFileImpl();
//                hdfsFile.init(anyString, anyString, (EventSerializer) any); times = 1;
                hdfsFile.write(events.get(i - 1)); times = 1;

                fileFactory.createFile(withSubstring("customer" + i), anyString); times = 1; result = hdfsFile;

                HdfsFileContext context = new HdfsFileContext(hdfsFile);
                context.getHdfsFile(); times = 1; result = hdfsFile;

                context.readLock(); times = 1;
                context.readUnlock(); times = 1;
            }
            closeExec.submit((Runnable) any); times = 2;
        }};

        plunker.setFileFactory(fileFactory);
        plunker.setCloseExec(closeExec);
        plunker.init(config);
        plunker.handleInternal(events);
        plunker.shutdown();
    }
}
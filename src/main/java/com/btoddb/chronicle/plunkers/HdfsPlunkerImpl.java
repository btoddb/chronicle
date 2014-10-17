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
import com.btoddb.chronicle.TokenizedFormatter;
import com.btoddb.chronicle.Utils;
import com.btoddb.chronicle.plunkers.hdfs.FileUtils;
import com.btoddb.chronicle.plunkers.hdfs.HdfsFile;
import com.btoddb.chronicle.plunkers.hdfs.HdfsFileContext;
import com.btoddb.chronicle.plunkers.hdfs.HdfsFileFactory;
import com.btoddb.chronicle.plunkers.hdfs.HdfsFileFactoryImpl;
import com.btoddb.chronicle.plunkers.hdfs.HdfsTextFileImpl;
import com.btoddb.chronicle.plunkers.hdfs.HdfsTokenValueProviderImpl;
import com.btoddb.chronicle.serializers.JsonSerializerImpl;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 *
 */
public class HdfsPlunkerImpl extends PlunkerBaseImpl {
    private static final Logger logger = LoggerFactory.getLogger(HdfsPlunkerImpl.class);

    private String fileType = HdfsTextFileImpl.class.getCanonicalName();
    private String serializerType = JsonSerializerImpl.class.getCanonicalName();
    private String pathPattern;
    private String permNamePattern;
    private String openNamePattern;
    private long rollPeriod = 600; // seconds (10 minutes)
    private long idleTimeout = 60; // seconds (1 minute)
    private int maxOpenFiles = 100;
    private int numIdleTimeoutThreads = 2;
    private int numCloseThreads = 4;
    private long shutdownWaitTimeout = 60; // seconds
    private int timeoutCheckPeriod = 10000; // millis

    private Cache<String, HdfsFileContext> fileCache;
    private FileUtils fileUtils = new FileUtils();

    private TokenizedFormatter keyTokenizedFilePath; // this is purely for HdfsFile lookups
    private TokenizedFormatter permTokenizedFilePath;
    private TokenizedFormatter openTokenizedFilePath;

    private ScheduledThreadPoolExecutor idleTimerExec;
    private ScheduledThreadPoolExecutor closeExec;

    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    private ReentrantReadWriteLock canHandleRequests = new ReentrantReadWriteLock();
    private HdfsFileFactory hdfsFileFactory;


    @Override
    public void init(Config config) throws Exception {
        super.init(config);

        createExecutors();
        createFilePatterns();
        createFileCache();

        if (null == this.hdfsFileFactory) {
            this.hdfsFileFactory = new HdfsFileFactoryImpl(config, fileType, serializerType);
        }

    }

    /**
     * Handle processing/saving events to HDFS.
     *
     * @param events collection of events
     * @throws Exception
     */
    @Override
    protected void handleInternal(Collection<Event> events) throws Exception {
        canHandleRequests.readLock().lock();
        try {
            if (isShutdown.get()) {
                logger.warn("rejecting request - plunker has been shutdown");
                return;
            }

            for (Event event : events) {
                // TODO:BTB - need some locking here to make sure we get a writer?
                HdfsFileContext context = retrieveHdfsFile(event);
                context.readLock();
                try {
                    context.getHdfsFile().write(event);
                    context.setLastAccessTime(System.currentTimeMillis());
                }
                finally {
                    context.readUnlock();
                }
            }
        }
        finally {
            canHandleRequests.readLock().unlock();
        }
    }

    // closing HDFS files is done on a thread because it can take some time
    // also, if the close operation throws an exception, we try again
    private void submitClose(final HdfsFileContext context) {
        closeExec.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // writer must handle thread-safe closing
                    context.getHdfsFile().close();
                }
                catch (IOException e) {
                    logger.error("exception while closing HdfsFile - retrying", e);
                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e1) {
                        // ignore
                        Thread.interrupted();
                    }
                    // wait one second then try again
                    closeExec.schedule(this, 1, TimeUnit.SECONDS);
                }
            }
        });
    }

    private HdfsFileContext retrieveHdfsFile(final Event event) {
        try {
            return fileCache.get(keyTokenizedFilePath.render(event), new Callable<HdfsFileContext>() {
                @Override
                public HdfsFileContext call() throws IOException {
                    HdfsTokenValueProviderImpl provider = new HdfsTokenValueProviderImpl();
                    String permFileName = permTokenizedFilePath.render(event, provider);
                    String openFileName = openTokenizedFilePath.render(event, provider);

                    HdfsFile hdfsFile = hdfsFileFactory.createFile(permFileName, openFileName);
                    return new HdfsFileContext(hdfsFile);
                }
            });
        }
        catch (ExecutionException e) {
            Utils.logAndThrow(logger, "exception while trying to retrieve HdfsFile from cache", e);
            return null;
        }
    }

    @Override
    public void shutdown() {
        if (!isShutdown.compareAndSet(false, true)) {
            logger.error("shutdown already called - returning");
            return;
        }

        idleTimerExec.shutdown();

        canHandleRequests.writeLock().lock();
        try {
            closeFilesAndWait();
        }
        finally {
            canHandleRequests.writeLock().unlock();
        }
    }

    private void closeFilesAndWait() {
        if (null != fileCache) {
            for (HdfsFileContext context : fileCache.asMap().values()) {
                submitClose(context);
            }
        }

        closeExec.shutdown();

        try {
            if (!closeExec.awaitTermination(shutdownWaitTimeout, TimeUnit.SECONDS)) {
                closeExec.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            logger.error("exception while waiting for HdfsFiles to close", e);
        }
    }

    void createFilePatterns() {
        keyTokenizedFilePath = new TokenizedFormatter(fileUtils.concatPath(pathPattern, permNamePattern));
        permTokenizedFilePath = new TokenizedFormatter(fileUtils.concatPath(pathPattern, fileUtils.insertTimestampPattern(permNamePattern)));
        openTokenizedFilePath = new TokenizedFormatter(fileUtils.concatPath(pathPattern, fileUtils.insertTimestampPattern(openNamePattern)));
    }

    private void createExecutors() {
        if (null == idleTimerExec) {
            idleTimerExec = new ScheduledThreadPoolExecutor(
                    numIdleTimeoutThreads,
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r);
                            t.setName("FPQ-HDFS-IdleTimeout");
                            return t;
                        }
                    }
            );
        }

        if (null == closeExec) {
            closeExec = new ScheduledThreadPoolExecutor(
                    numCloseThreads,
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r);
                            t.setName("FPQ-HDFS-Closer");
                            return t;
                        }
                    },
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
        }

        // start scheduled idle task that checks if time to close file because of roll period or idle timeout
        idleTimerExec.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    // if any writer has been idle for longer than the "idle timeout"
                    // or the roll period has been exceeded, then close the file.  it
                    // will be reopened as needed when another request comes
                    long rollPeriodCutoff = System.currentTimeMillis()-TimeUnit.SECONDS.toMillis(rollPeriod);
                    long lastAccessCutoff = System.currentTimeMillis()-TimeUnit.SECONDS.toMillis(idleTimeout);
                    for (String key : fileCache.asMap().keySet()) {
                        HdfsFileContext context = fileCache.getIfPresent(key);
                        if (null != context && context.isActive()
                                // if idle timeout == 0, then don't consider
                                && (rollPeriodCutoff > context.getCreateTime()
                                    || (idleTimeout > 0 && lastAccessCutoff > context.getLastAccessTime()))) {
                            context.writeLock();
                            try {
                                // if current thread is the one that got the write lock while still active, then we
                                // make it inactive so no other thread will do the same and start close procedure
                                if (context.isActive()) {
                                    context.setActive(false);
                                    // invalidating the cache will cause the writer to be closed
                                    fileCache.invalidate(key);
                                }
                            }
                            finally {
                                context.writeUnlock();
                            }
                        }
                    }
                }
            },
            timeoutCheckPeriod, timeoutCheckPeriod, TimeUnit.MILLISECONDS); // check periodically if file needs closing
    }

    private void createFileCache() {
        fileCache = CacheBuilder.newBuilder()
                .maximumSize(maxOpenFiles)
                .removalListener(new RemovalListener<String, HdfsFileContext>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, HdfsFileContext> entry) {
                        submitClose(entry.getValue());
                    }
                })
                .build();
    }

    public String getPathPattern() {
        return pathPattern;
    }

    public void setPathPattern(String pathPattern) {
        this.pathPattern = pathPattern;
    }

    public String getPermNamePattern() {
        return permNamePattern;
    }

    public void setPermNamePattern(String permNamePattern) {
        this.permNamePattern = permNamePattern;
    }

    public String getOpenNamePattern() {
        return openNamePattern;
    }

    public void setOpenNamePattern(String openNamePattern) {
        this.openNamePattern = openNamePattern;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }

    public void setMaxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
    }

    public String getSerializerType() {
        return serializerType;
    }

    public void setSerializerType(String serializerType) {
        this.serializerType = serializerType;
    }

    public long getRollPeriod() {
        return rollPeriod;
    }

    public void setRollPeriod(int rollPeriod) {
        this.rollPeriod = rollPeriod;
    }

    public HdfsFileFactory getHdfsFileFactory() {
        return hdfsFileFactory;
    }

    public void setHdfsFileFactory(HdfsFileFactory hdfsFileFactory) {
        this.hdfsFileFactory = hdfsFileFactory;
    }

    public void setRollTimeout(long rollTimeout) {
        this.rollPeriod = rollTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public int getNumIdleTimeoutThreads() {
        return numIdleTimeoutThreads;
    }

    public void setNumIdleTimeoutThreads(int numIdleTimeoutThreads) {
        this.numIdleTimeoutThreads = numIdleTimeoutThreads;
    }

    public int getNumCloseThreads() {
        return numCloseThreads;
    }

    public void setNumCloseThreads(int numCloseThreads) {
        this.numCloseThreads = numCloseThreads;
    }

    ScheduledThreadPoolExecutor getIdleTimerExec() {
        return idleTimerExec;
    }

    void setIdleTimerExec(ScheduledThreadPoolExecutor idleTimerExec) {
        this.idleTimerExec = idleTimerExec;
    }

    ScheduledThreadPoolExecutor getCloseExec() {
        return closeExec;
    }

    void setCloseExec(ScheduledThreadPoolExecutor closeExec) {
        this.closeExec = closeExec;
    }

    TokenizedFormatter getPermTokenizedFilePath() {
        return permTokenizedFilePath;
    }

    TokenizedFormatter getOpenTokenizedFilePath() {
        return openTokenizedFilePath;
    }

    TokenizedFormatter getKeyTokenizedFilePath() {
        return keyTokenizedFilePath;
    }

    public long getShutdownWaitTimeout() {
        return shutdownWaitTimeout;
    }

    public void setShutdownWaitTimeout(long shutdownWaitTimeout) {
        this.shutdownWaitTimeout = shutdownWaitTimeout;
    }

    public Collection<HdfsFileContext> getFiles() {
        return fileCache.asMap().values();
    }

    public int getTimeoutCheckPeriod() {
        return timeoutCheckPeriod;
    }

    public void setTimeoutCheckPeriod(int timeoutCheckPeriod) {
        this.timeoutCheckPeriod = timeoutCheckPeriod;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }
}

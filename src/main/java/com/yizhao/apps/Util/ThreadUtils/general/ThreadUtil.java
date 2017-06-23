package com.yizhao.apps.Util.ThreadUtils.general;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * Created by yzhao on 4/11/17.
 */
public class ThreadUtil {
    private static final Logger logger = Logger.getLogger(ThreadUtil.class);

    public static void stopAllThreads(Map<String, ExecutorService> threadPools, String serviceDescription, long waitTime, TimeUnit waitTimeUnit) {
        for (String poolName : threadPools.keySet()) {
            ThreadUtil.forceShutdownAfterWaiting(threadPools.get(poolName),
                    serviceDescription, waitTime, waitTimeUnit);
        }
    }


    /**
     * ThreadUtil.newThread(threadPools, fileSourceInput, true, Thread.NORM_PRIORITY);
     *
     * @param threadPools
     * @param threadName
     * @param daemon
     * @param threadPriority
     * @return
     */
    public static ExecutorService newThread(Map<String, ExecutorService> threadPools, String threadName, boolean daemon, int threadPriority) {
        ExecutorService threadPool = threadPools.get(threadName);
        if (threadPool == null) {
            BasicThreadFactory factory = new BasicThreadFactory.Builder()
                    .namingPattern(threadName + "-%d")

                    .daemon(daemon)
                    .priority(threadPriority)
                    .build();
            threadPool = Executors.newSingleThreadExecutor(factory);
            threadPools.put(threadName, threadPool);
        }
        return threadPool;
    }

    /**
     * Force shutdown (if required) an executor service after waiting for the
     * given amount of time.
     *
     * @param executorService
     * @param serviceDescription
     * @param waitTime
     * @param waitTimeUnit
     * @return list of runnable threads that remained after a forced shutdown.
     */
    public static List<Runnable> forceShutdownAfterWaiting(ExecutorService executorService,
                                                           String serviceDescription, long waitTime, TimeUnit waitTimeUnit) {
        List<Runnable> remainingThreads = Collections.<Runnable>emptyList();
        if (executorService != null && !executorService.isTerminated()) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(waitTime, waitTimeUnit);
            } catch (InterruptedException e) {
                logger.debug("The service <" + serviceDescription + "> was interrupted while waiting termination");
            }
            if (!executorService.isTerminated()) {
                remainingThreads = executorService.shutdownNow();
                if (remainingThreads.size() > 0) {
                    logger.debug("Threads remaining after a forced shut down for " + serviceDescription + ": " + remainingThreads);
                }
            }
        }
        return remainingThreads;
    }

    public static void shutdown(ExecutorService executorService, String serviceDescription) {
        if (executorService != null && !executorService.isTerminated()) {
            logger.info("shutting down executor " + serviceDescription);
            executorService.shutdown();
        }
    }
}
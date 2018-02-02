package com.github.andy.buffer.group;

import java.util.concurrent.*;

/**
 * 缓冲处理器的创建器
 * <p>
 * Created by yanshanguang on 18/1/30.
 */
public class BufferGroupProcessorCreator<E, G, R> {

    private static int DEFAULT_PROCESS_EXECUTOR_THREADS = 10;
    private static int DEFAULT_PROCESS_EXECUTOR_QUEUE_SIZE = 1000;

    private final int bufferQueueSize;
    private final int consumeBatchSize;
    private final int maxConsumeIntervalSleepMs;
    private final BufferGroupStrategy<E, G> bufferGroupStrategy;
    private final BufferGroupHandler<E, G, R> bufferGroupHandler;
    private final BufferProcessExecutorFactory bufferProcessExecutorFactory;

    public BufferGroupProcessorCreator(int bufferQueueSize,
                                       int consumeBatchSize,
                                       int maxConsumeIntervalSleepMs,
                                       BufferGroupStrategy<E, G> bufferGroupStrategy,
                                       BufferGroupHandler<E, G, R> bufferGroupHandler,
                                       BufferProcessExecutorFactory bufferProcessExecutorFactory) {
        this.bufferQueueSize = bufferQueueSize;
        this.consumeBatchSize = consumeBatchSize;
        this.maxConsumeIntervalSleepMs = maxConsumeIntervalSleepMs;
        this.bufferGroupStrategy = bufferGroupStrategy;
        this.bufferGroupHandler = bufferGroupHandler;
        this.bufferProcessExecutorFactory = bufferProcessExecutorFactory;
    }

    public BufferGroupProcessor<E, G, R> get() {
        return new BufferGroupProcessor<E, G, R>(bufferQueueSize, consumeBatchSize,
                maxConsumeIntervalSleepMs, bufferGroupStrategy, bufferGroupHandler, newBufferProcessExecutor());
    }

    private ExecutorService newBufferProcessExecutor() {
        if (bufferProcessExecutorFactory != null) {
            return bufferProcessExecutorFactory.create();
        }

        return newDefaultBufferProcessExecutor();
    }

    private ExecutorService newDefaultBufferProcessExecutor() {
        return new ThreadPoolExecutor(DEFAULT_PROCESS_EXECUTOR_THREADS,
                DEFAULT_PROCESS_EXECUTOR_THREADS,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(DEFAULT_PROCESS_EXECUTOR_QUEUE_SIZE),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

}
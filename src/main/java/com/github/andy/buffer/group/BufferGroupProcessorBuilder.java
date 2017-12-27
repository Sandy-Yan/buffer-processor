package com.github.andy.buffer.group;

import com.google.common.base.Preconditions;

import java.util.concurrent.*;

/**
 * Created by andy on 17/12/12.
 */
public class BufferGroupProcessorBuilder<E, G, R> {

    private static int DEFAULT_PROCESS_EXECUTOR_THREADS = 10;
    private static int DEFAULT_PROCESS_EXECUTOR_QUEUE_SIZE = 1000;

    private int bufferQueueSize;
    private int consumeBatchSize;
    private BufferGroupStrategy<E, G> bufferGroupStrategy;
    private BufferGroupHandler<E, R> bufferGroupHandler;
    private ExecutorService bufferProcessExecutor;

    public BufferGroupProcessorBuilder<E, G, R> bufferQueueSize(int bufferQueueSize) {
        this.bufferQueueSize = bufferQueueSize;
        return this;
    }

    public BufferGroupProcessorBuilder<E, G, R> consumeBatchSize(int consumeBatchSize) {
        this.consumeBatchSize = consumeBatchSize;
        return this;
    }

    public BufferGroupProcessorBuilder<E, G, R> bufferGroupStrategy(BufferGroupStrategy<E, G> bufferGroupStrategy) {
        this.bufferGroupStrategy = bufferGroupStrategy;
        return this;
    }

    public BufferGroupProcessorBuilder<E, G, R> bufferGroupHandler(BufferGroupHandler<E, R> bufferGroupHandler) {
        this.bufferGroupHandler = bufferGroupHandler;
        return this;
    }

    public BufferGroupProcessorBuilder<E, G, R> bufferProcessExecutor(ExecutorService bufferProcessExecutor) {
        this.bufferProcessExecutor = bufferProcessExecutor;
        return this;
    }


    public BufferGroupProcessor<E, G, R> build() {
        check();
        return newBufferGroupProcessor();
    }

    private void check() {

        Preconditions.checkArgument(bufferQueueSize > 0,
                "必须设置项bufferQueueSize必须大于0。");

        Preconditions.checkArgument(consumeBatchSize > 0,
                "必须设置项consumeBatchSize必须大于0。");

        Preconditions.checkArgument(bufferGroupStrategy != null,
                "必须设置项bufferGroupStrategy为Null。");

        Preconditions.checkArgument(bufferGroupHandler != null,
                "必须设置项bufferGroupHandler为Null。");

    }

    private BufferGroupProcessor<E, G, R> newBufferGroupProcessor() {
        return new BufferGroupProcessor<E, G, R>(bufferQueueSize, consumeBatchSize,
                bufferGroupStrategy, bufferGroupHandler, newBufferProcessExecutor());
    }

    private ExecutorService newBufferProcessExecutor() {
        if (bufferProcessExecutor != null) {
            return bufferProcessExecutor;
        }

        return new ThreadPoolExecutor(DEFAULT_PROCESS_EXECUTOR_THREADS,
                DEFAULT_PROCESS_EXECUTOR_THREADS,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(DEFAULT_PROCESS_EXECUTOR_QUEUE_SIZE),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }
}

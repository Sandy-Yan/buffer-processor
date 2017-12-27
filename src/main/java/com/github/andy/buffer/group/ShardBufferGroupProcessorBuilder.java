package com.github.andy.buffer.group;

import com.google.common.base.Preconditions;

import java.util.concurrent.*;

/**
 * Created by yanshanguang on 17/12/11.
 */
public class ShardBufferGroupProcessorBuilder<E, G, R> {

    private static int DEFAULT_PROCESS_EXECUTOR_THREADS = 10;
    private static int DEFAULT_PROCESS_EXECUTOR_QUEUE_SIZE = 1000;

    private int bufferQueueSize;
    private int consumeBatchSize;
    private BufferGroupStrategy<E, G> bufferGroupStrategy;
    private BufferGroupHandler<E, R> bufferGroupHandler;
    private int shardGroupProcessorSize;
    private ShardBufferProcessorStrategy<E> shardBufferProcessorStrategy;
    private BufferProcessExecutorFactory bufferProcessExecutorFactory;

    public ShardBufferGroupProcessorBuilder<E, G, R> bufferQueueSize(int bufferQueueSize) {
        this.bufferQueueSize = bufferQueueSize;
        return this;
    }

    public ShardBufferGroupProcessorBuilder<E, G, R> consumeBatchSize(int consumeBatchSize) {
        this.consumeBatchSize = consumeBatchSize;
        return this;
    }

    public ShardBufferGroupProcessorBuilder<E, G, R> bufferGroupStrategy(BufferGroupStrategy<E, G> bufferGroupStrategy) {
        this.bufferGroupStrategy = bufferGroupStrategy;
        return this;
    }

    public ShardBufferGroupProcessorBuilder<E, G, R> bufferGroupHandler(BufferGroupHandler<E, R> bufferGroupHandler) {
        this.bufferGroupHandler = bufferGroupHandler;
        return this;
    }

    public ShardBufferGroupProcessorBuilder<E, G, R> bufferProcessExecutorFactory(BufferProcessExecutorFactory bufferProcessExecutorFactory) {
        this.bufferProcessExecutorFactory = bufferProcessExecutorFactory;
        return this;
    }

    public ShardBufferGroupProcessorBuilder<E, G, R> shardGroupProcessorSize(int shardGroupProcessorSize) {
        this.shardGroupProcessorSize = shardGroupProcessorSize;
        return this;
    }

    public ShardBufferGroupProcessorBuilder<E, G, R> shardBufferProcessorStrategy(ShardBufferProcessorStrategy<E> shardBufferProcessorStrategy) {
        this.shardBufferProcessorStrategy = shardBufferProcessorStrategy;
        return this;
    }

    public ShardBufferGroupProcessor<E, G, R> build() {
        check();

        BufferGroupProcessor<E, G, R>[] bufferGroupProcessors = newBufferGroupProcessors();

        return new ShardBufferGroupProcessor<E, G, R>(bufferGroupProcessors, shardBufferProcessorStrategy);
    }

    private void check() {

        Preconditions.checkArgument(shardGroupProcessorSize > 0,
                "必须设置项shardGroupProcessorSize必须大于0。");

        Preconditions.checkArgument(shardBufferProcessorStrategy != null,
                "必须设置项shardBufferProcessorStrategy为Null。");

        Preconditions.checkArgument(bufferQueueSize > 0,
                "必须设置项bufferQueueSize必须大于0。");

        Preconditions.checkArgument(consumeBatchSize > 0,
                "必须设置项consumeBatchSize必须大于0。");

        Preconditions.checkArgument(bufferGroupStrategy != null,
                "必须设置项bufferGroupStrategy为Null。");

        Preconditions.checkArgument(bufferGroupHandler != null,
                "必须设置项bufferGroupHandler为Null。");

    }

    private BufferGroupProcessor<E, G, R>[] newBufferGroupProcessors() {
        BufferGroupProcessor<E, G, R>[] bufferGroupProcessors = new BufferGroupProcessor[shardGroupProcessorSize];
        for (int i = 0; i < shardGroupProcessorSize; i++) {
            bufferGroupProcessors[i] = new BufferGroupProcessor<E, G, R>(bufferQueueSize, consumeBatchSize,
                    bufferGroupStrategy, bufferGroupHandler, newBufferProcessExecutor());
        }

        return bufferGroupProcessors;
    }

    private ExecutorService newBufferProcessExecutor() {
        if (bufferProcessExecutorFactory != null) {
            return bufferProcessExecutorFactory.create();
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

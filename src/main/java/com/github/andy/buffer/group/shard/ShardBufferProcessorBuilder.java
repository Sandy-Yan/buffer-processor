package com.github.andy.buffer.group.shard;

import com.github.andy.buffer.group.*;
import com.google.common.base.Preconditions;

/**
 * Created by yanshanguang on 17/12/11.
 */
public class ShardBufferProcessorBuilder<E, G, R> {

    private int bufferQueueSize;
    private int consumeBatchSize;
    private int maxConsumeIntervalSleepMs;
    private BufferGroupStrategy<E, G> bufferGroupStrategy;
    private BufferGroupHandler<E, G, R> bufferGroupHandler;
    private BufferProcessExecutorFactory bufferProcessExecutorFactory;
    private int shardBufferProcessorSize;
    private ShardBufferProcessorStrategy<E> shardBufferProcessorStrategy;

    public ShardBufferProcessorBuilder<E, G, R> bufferQueueSize(int bufferQueueSize) {
        this.bufferQueueSize = bufferQueueSize;
        return this;
    }

    public ShardBufferProcessorBuilder<E, G, R> consumeBatchSize(int consumeBatchSize) {
        this.consumeBatchSize = consumeBatchSize;
        return this;
    }

    public ShardBufferProcessorBuilder<E, G, R> maxConsumeIntervalSleepMs(int maxConsumeIntervalSleepMs) {
        this.maxConsumeIntervalSleepMs = maxConsumeIntervalSleepMs;
        return this;
    }

    public ShardBufferProcessorBuilder<E, G, R> bufferGroupStrategy(BufferGroupStrategy<E, G> bufferGroupStrategy) {
        this.bufferGroupStrategy = bufferGroupStrategy;
        return this;
    }

    public ShardBufferProcessorBuilder<E, G, R> bufferGroupHandler(BufferGroupHandler<E, G, R> bufferGroupHandler) {
        this.bufferGroupHandler = bufferGroupHandler;
        return this;
    }

    public ShardBufferProcessorBuilder<E, G, R> bufferProcessExecutorFactory(BufferProcessExecutorFactory bufferProcessExecutorFactory) {
        this.bufferProcessExecutorFactory = bufferProcessExecutorFactory;
        return this;
    }

    public ShardBufferProcessorBuilder<E, G, R> shardBufferProcessorSize(int shardBufferProcessorSize) {
        this.shardBufferProcessorSize = shardBufferProcessorSize;
        return this;
    }

    public ShardBufferProcessorBuilder<E, G, R> shardBufferProcessorStrategy(ShardBufferProcessorStrategy<E> shardBufferProcessorStrategy) {
        this.shardBufferProcessorStrategy = shardBufferProcessorStrategy;
        return this;
    }

    public ShardBufferProcessor<E, G, R> build() {
        check();
        BufferGroupProcessor<E, G, R>[] bufferProcessors = newBufferProcessors();
        return new ShardBufferProcessor<E, G, R>(bufferProcessors, shardBufferProcessorStrategy);
    }

    private void check() {

        Preconditions.checkArgument(shardBufferProcessorSize > 0,
                "必须设置项shardBufferProcessorSize必须大于0。");

        Preconditions.checkArgument(shardBufferProcessorStrategy != null,
                "必须设置项shardBufferProcessorStrategy为Null。");

        Preconditions.checkArgument(bufferQueueSize > 0,
                "必须设置项bufferQueueSize必须大于0。");

        Preconditions.checkArgument(consumeBatchSize > 0,
                "必须设置项consumeBatchSize必须大于0。");

        Preconditions.checkArgument(maxConsumeIntervalSleepMs >= 0,
                "必须设置项maxConsumeIntervalSleepMs必须大于等于0。");

        Preconditions.checkArgument(bufferGroupHandler != null,
                "必须设置项bufferGroupHandler为Null。");

    }

    private BufferGroupProcessor<E, G, R>[] newBufferProcessors() {
        BufferGroupProcessor<E, G, R>[] bufferGroupProcessors = new BufferGroupProcessor[shardBufferProcessorSize];
        BufferGroupProcessorCreator<E, G, R> bufferProcessorCreator = newBufferProcessorCreator();
        for (int i = 0; i < shardBufferProcessorSize; i++) {
            bufferGroupProcessors[i] = bufferProcessorCreator.get();
        }

        return bufferGroupProcessors;
    }

    private BufferGroupProcessorCreator<E, G, R> newBufferProcessorCreator() {
        return new BufferGroupProcessorCreator<E, G, R>(bufferQueueSize, consumeBatchSize,
                maxConsumeIntervalSleepMs, bufferGroupStrategy, bufferGroupHandler, bufferProcessExecutorFactory);
    }

}
package com.github.andy.buffer.group;

import com.google.common.base.Preconditions;

/**
 * Created by yanshanguang on 17/12/12.
 */
public class BufferGroupProcessorBuilder<E, G, R> {

    private int bufferQueueSize;
    private int consumeBatchSize;
    private int maxConsumeIntervalSleepMs;
    private BufferGroupStrategy<E, G> bufferGroupStrategy;
    private BufferGroupHandler<E, G, R> bufferGroupHandler;
    private BufferProcessExecutorFactory bufferProcessExecutorFactory;

    public BufferGroupProcessorBuilder<E, G, R> bufferQueueSize(int bufferQueueSize) {
        this.bufferQueueSize = bufferQueueSize;
        return this;
    }

    public BufferGroupProcessorBuilder<E, G, R> consumeBatchSize(int consumeBatchSize) {
        this.consumeBatchSize = consumeBatchSize;
        return this;
    }

    public BufferGroupProcessorBuilder<E, G, R> maxConsumeIntervalSleepMs(int maxConsumeIntervalSleepMs) {
        this.maxConsumeIntervalSleepMs = maxConsumeIntervalSleepMs;
        return this;
    }

    public BufferGroupProcessorBuilder<E, G, R> bufferGroupStrategy(BufferGroupStrategy<E, G> bufferGroupStrategy) {
        this.bufferGroupStrategy = bufferGroupStrategy;
        return this;
    }

    public BufferGroupProcessorBuilder<E, G, R> bufferGroupHandler(BufferGroupHandler<E, G, R> bufferGroupHandler) {
        this.bufferGroupHandler = bufferGroupHandler;
        return this;
    }

    public BufferGroupProcessorBuilder<E, G, R> bufferProcessExecutorFactory(BufferProcessExecutorFactory bufferProcessExecutorFactory) {
        this.bufferProcessExecutorFactory = bufferProcessExecutorFactory;
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

        Preconditions.checkArgument(maxConsumeIntervalSleepMs >= 0,
                "必须设置项maxConsumeIntervalSleepMs必须大于等于0。");

        Preconditions.checkArgument(bufferGroupStrategy != null,
                "必须设置项bufferGroupStrategy为Null。");

        Preconditions.checkArgument(bufferGroupHandler != null,
                "必须设置项bufferGroupHandler为Null。");

    }

    private BufferGroupProcessor<E, G, R> newBufferGroupProcessor() {
        return newBufferProcessorCreator().get();
    }

    private BufferGroupProcessorCreator<E, G, R> newBufferProcessorCreator() {
        return new BufferGroupProcessorCreator<E, G, R>(bufferQueueSize, consumeBatchSize,
                maxConsumeIntervalSleepMs, bufferGroupStrategy, bufferGroupHandler, bufferProcessExecutorFactory);
    }

}
package com.github.andy.buffer.group.shard;

import com.github.andy.buffer.group.BufferGroupHandler;
import com.github.andy.buffer.group.BufferGroupProcessorCreator;
import com.github.andy.buffer.group.BufferGroupStrategy;
import com.github.andy.buffer.group.BufferProcessExecutorFactory;
import com.google.common.base.Preconditions;

/**
 * Created by yanshanguang on 18/1/30.
 */
public class ShardBufferProcessorBuilderV2<E, G, R, SK> {

    private int bufferQueueSize;
    private int consumeBatchSize;
    private int maxConsumeIntervalSleepMs;
    private BufferGroupStrategy<E, G> bufferGroupStrategy;
    private BufferGroupHandler<E, G, R> bufferGroupHandler;
    private BufferProcessExecutorFactory bufferProcessExecutorFactory;
    private ShardBufferProcessorIniter<E, G, R, SK> shardBufferProcessorIniter;
    private ShardBufferProcessorStrategyV2<E, SK> shardBufferProcessorStrategy;
    private BufferProcessorNotFoundCallback<E, G, R, SK> bufferProcessorNotFoundCallback;

    public ShardBufferProcessorBuilderV2<E, G, R, SK> bufferQueueSize(int bufferQueueSize) {
        this.bufferQueueSize = bufferQueueSize;
        return this;
    }

    public ShardBufferProcessorBuilderV2<E, G, R, SK> consumeBatchSize(int consumeBatchSize) {
        this.consumeBatchSize = consumeBatchSize;
        return this;
    }

    public ShardBufferProcessorBuilderV2<E, G, R, SK> maxConsumeIntervalSleepMs(int maxConsumeIntervalSleepMs) {
        this.maxConsumeIntervalSleepMs = maxConsumeIntervalSleepMs;
        return this;
    }

    public ShardBufferProcessorBuilderV2<E, G, R, SK> bufferGroupStrategy(BufferGroupStrategy<E, G> bufferGroupStrategy) {
        this.bufferGroupStrategy = bufferGroupStrategy;
        return this;
    }

    public ShardBufferProcessorBuilderV2<E, G, R, SK> bufferGroupHandler(BufferGroupHandler<E, G, R> bufferGroupHandler) {
        this.bufferGroupHandler = bufferGroupHandler;
        return this;
    }

    public ShardBufferProcessorBuilderV2<E, G, R, SK> bufferProcessExecutorFactory(BufferProcessExecutorFactory bufferProcessExecutorFactory) {
        this.bufferProcessExecutorFactory = bufferProcessExecutorFactory;
        return this;
    }

    public ShardBufferProcessorBuilderV2<E, G, R, SK> shardBufferProcessorIniter(ShardBufferProcessorIniter<E, G, R, SK> shardBufferProcessorIniter) {
        this.shardBufferProcessorIniter = shardBufferProcessorIniter;
        return this;
    }

    public ShardBufferProcessorBuilderV2<E, G, R, SK> shardBufferProcessorStrategy(ShardBufferProcessorStrategyV2<E, SK> shardBufferProcessorStrategy) {
        this.shardBufferProcessorStrategy = shardBufferProcessorStrategy;
        return this;
    }

    public ShardBufferProcessorBuilderV2<E, G, R, SK> bufferProcessorNotFoundCallback(BufferProcessorNotFoundCallback<E, G, R, SK> bufferProcessorNotFoundCallback) {
        this.bufferProcessorNotFoundCallback = bufferProcessorNotFoundCallback;
        return this;
    }

    public ShardBufferProcessorV2<E, G, R, SK> build() {
        check();

        // 构建及初始化缓冲处理器的容器
        ShardBufferProcessorContainer<E, G, R, SK> bufferProcessorContainer = newAndInitBufferProcessorContainer();

        return new ShardBufferProcessorV2<E, G, R, SK>(bufferProcessorContainer, shardBufferProcessorStrategy, bufferProcessorNotFoundCallback);
    }

    private void check() {

        Preconditions.checkArgument(bufferQueueSize > 0,
                "必须设置项bufferQueueSize必须大于0。");

        Preconditions.checkArgument(consumeBatchSize > 0,
                "必须设置项consumeBatchSize必须大于0。");

        Preconditions.checkArgument(maxConsumeIntervalSleepMs >= 0,
                "必须设置项maxConsumeIntervalSleepMs必须大于等于0。");

        Preconditions.checkArgument(bufferGroupHandler != null,
                "必须设置项bufferGroupHandler为Null。");

        Preconditions.checkArgument(shardBufferProcessorStrategy != null,
                "必须设置项shardBufferProcessorStrategy为Null。");

    }

    private ShardBufferProcessorContainer<E, G, R, SK> newAndInitBufferProcessorContainer() {
        // 创建缓冲处理器的管理容器
        ShardBufferProcessorContainer<E, G, R, SK> bufferProcessorContainer = newBufferProcessorContainer();

        // 初始化启动所需的缓冲处理器
        initBufferProcessors(bufferProcessorContainer);

        return bufferProcessorContainer;
    }

    private ShardBufferProcessorContainer<E, G, R, SK> newBufferProcessorContainer() {
        // 创建缓冲处理器的构造器
        BufferGroupProcessorCreator<E, G, R> bufferProcessorCreator = newBufferProcessorCreator();
        return new ShardBufferProcessorContainer<>(bufferProcessorCreator);
    }

    private BufferGroupProcessorCreator<E, G, R> newBufferProcessorCreator() {
        return new BufferGroupProcessorCreator<E, G, R>(bufferQueueSize, consumeBatchSize,
                maxConsumeIntervalSleepMs, bufferGroupStrategy, bufferGroupHandler, bufferProcessExecutorFactory);
    }

    private void initBufferProcessors(ShardBufferProcessorContainer<E, G, R, SK> bufferProcessorContainer) {
        if (shardBufferProcessorIniter != null) {
            shardBufferProcessorIniter.init(bufferProcessorContainer);
        }
    }

}
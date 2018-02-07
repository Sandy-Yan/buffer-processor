package com.github.andy.buffer.group.shard;

import com.github.andy.buffer.group.BufferFuture;
import com.github.andy.buffer.group.BufferGroupProcessor;

/**
 * Shard实体缓冲分组处理器V2
 * <p>
 * Created by yanshanguang on 18/1/30.
 */
public class ShardBufferProcessorV2<E, G, R, SK> {

    private final ShardBufferProcessorContainer<E, G, R, SK> bufferProcessorContainer;

    private final ShardBufferProcessorStrategyV2<E, SK> shardBufferProcessorStrategy;

    private final BufferProcessorNotFoundCallback<E, G, R, SK> bufferProcessorNotFoundCallback;

    public ShardBufferProcessorV2(ShardBufferProcessorContainer<E, G, R, SK> bufferProcessorContainer,
                                ShardBufferProcessorStrategyV2<E, SK> shardBufferProcessorStrategy,
                                BufferProcessorNotFoundCallback<E, G, R, SK> bufferProcessorNotFoundCallback) {
        this.bufferProcessorContainer = bufferProcessorContainer;
        this.shardBufferProcessorStrategy = shardBufferProcessorStrategy;
        this.bufferProcessorNotFoundCallback = bufferProcessorNotFoundCallback;
    }

    public static <E, G, R, SK> ShardBufferProcessorBuilderV2<E, G, R, SK> newBuilder() {
        return new ShardBufferProcessorBuilderV2<E, G, R, SK>();
    }

    /**
     * 提交请求数据到缓冲处理器入口
     *
     * @param element
     * @return
     * @throws InterruptedException
     */
    public BufferFuture<R> submit(E element) throws InterruptedException {
        BufferGroupProcessor<E, G, R> processor = routeProcessor(element);
        return processor.submit(element);
    }

    public int getProcessorCount() {
        return bufferProcessorContainer.getProcessorCount();
    }

    private BufferGroupProcessor<E, G, R> routeProcessor(E element) {

        // 路由缓冲处理器的KEY
        SK shardKey = shardBufferProcessorStrategy.routeSK(element);

        // 用路由KEY取缓冲处理器
        BufferGroupProcessor<E, G, R> bufferProcessor = bufferProcessorContainer.get(shardKey);

        if (bufferProcessor != null) {
            return bufferProcessor;
        }

        if (bufferProcessorNotFoundCallback != null) {
            bufferProcessorNotFoundCallback.execute(bufferProcessorContainer, element);
            bufferProcessor = bufferProcessorContainer.get(shardKey);
        }

        if (bufferProcessor == null) {
            throw new RuntimeException("[ShardBufferProcessor] 路由缓冲处理器，结果未找到！");
        }

        return bufferProcessor;
    }

}
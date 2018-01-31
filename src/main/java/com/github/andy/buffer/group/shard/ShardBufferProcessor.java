package com.github.andy.buffer.group.shard;

import com.github.andy.buffer.group.BufferFuture;
import com.github.andy.buffer.group.BufferGroupProcessor;

/**
 * Shard实体缓冲分组处理器
 * <p>
 * Created by yanshanguang on 17/12/11.
 */
public class ShardBufferProcessor<E, G, R> {

    private final BufferGroupProcessor<E, G, R>[] bufferProcessors;

    private final ShardBufferProcessorStrategy<E> shardBufferProcessorStrategy;

    public ShardBufferProcessor(BufferGroupProcessor<E, G, R>[] bufferProcessors,
                                ShardBufferProcessorStrategy<E> shardBufferProcessorStrategy) {
        this.bufferProcessors = bufferProcessors;
        this.shardBufferProcessorStrategy = shardBufferProcessorStrategy;
    }

    public static <E, G, R> ShardBufferProcessorBuilder<E, G, R> newBuilder() {
        return new ShardBufferProcessorBuilder<E, G, R>();
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

    private BufferGroupProcessor<E, G, R> routeProcessor(E element) {
        int processorsCount = bufferProcessors.length;
        int processorsIndex = shardBufferProcessorStrategy.routeIDX(processorsCount, element);

        if (processorsIndex < 0 || processorsIndex >= processorsCount) {
            throw new RuntimeException("[ShardBufferProcessor] 路由缓冲处理器的索引不在实际索引范围内！");
        }

        return bufferProcessors[processorsIndex];
    }

}
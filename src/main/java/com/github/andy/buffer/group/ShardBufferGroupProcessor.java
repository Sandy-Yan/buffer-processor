package com.github.andy.buffer.group;

/**
 * Shard实体缓冲分组处理器
 * <p>
 * Created by yanshanguang on 17/12/11.
 */
public class ShardBufferGroupProcessor<E, G, R> {

    private final BufferGroupProcessor<E, G, R>[] groupProcessors;

    private final ShardBufferProcessorStrategy<E> shardBufferProcessorStrategy;

    public ShardBufferGroupProcessor(BufferGroupProcessor<E, G, R>[] groupProcessors,
                                     ShardBufferProcessorStrategy<E> shardBufferProcessorStrategy) {
        this.groupProcessors = groupProcessors;
        this.shardBufferProcessorStrategy = shardBufferProcessorStrategy;
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
        int processorsCount = groupProcessors.length;
        int processorsIndex = shardBufferProcessorStrategy.routeProcessorIDX(processorsCount, element);

        if (processorsIndex < 0 || processorsIndex >= processorsCount) {
            throw new RuntimeException("路由缓冲分组处理器的索引不在实际索引范围内！");
        }

        return groupProcessors[processorsIndex];
    }

}

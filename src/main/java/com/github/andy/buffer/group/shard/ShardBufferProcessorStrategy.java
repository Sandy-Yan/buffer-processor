package com.github.andy.buffer.group.shard;

/**
 * ShardBufferProcessor的路由策略
 * <p>
 * Created by yanshanguang on 17/12/11.
 */
public interface ShardBufferProcessorStrategy<E> {

    int routeIDX(int processorsCount, E element);
}

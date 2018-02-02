package com.github.andy.buffer.group.shard;

/**
 * ShardBufferProcessor的路由策略V2
 * <p>
 * Created by yanshanguang on 18/1/30.
 */
public interface ShardBufferProcessorStrategyV2<E, SK> {

    SK routeSK(final E element);
}

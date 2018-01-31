package com.github.andy.buffer.group.shard;

/**
 * ShardBufferProcessor处理器的初始化执行接口
 * <p>
 * Created by yanshanguang on 18/1/31.
 */
public interface ShardBufferProcessorIniter<E, G, R, SK> {

    void init(ShardBufferProcessorContainer<E, G, R, SK> bufferProcessorContainer);
}

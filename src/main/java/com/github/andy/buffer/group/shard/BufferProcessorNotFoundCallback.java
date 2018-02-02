package com.github.andy.buffer.group.shard;

/**
 * Created by yanshanguang on 18/2/3.
 */
public interface BufferProcessorNotFoundCallback<E, G, R, SK> {

    void execute(ShardBufferProcessorContainer<E, G, R, SK> bufferProcessorContainer, E element);
}

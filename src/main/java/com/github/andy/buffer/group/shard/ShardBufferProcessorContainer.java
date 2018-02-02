package com.github.andy.buffer.group.shard;

import com.github.andy.buffer.group.BufferGroupProcessor;
import com.github.andy.buffer.group.BufferGroupProcessorCreator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ShardBufferProcessor的处理器的管理容器
 * <p>
 * Created by yanshanguang on 18/1/31.
 */
public class ShardBufferProcessorContainer<E, G, R, SK> {

    private final ConcurrentMap<SK, BufferProcessorLazyer> bufferProcessorsMap = new ConcurrentHashMap<>();

    private final BufferGroupProcessorCreator<E, G, R> bufferProcessorCreator;

    public ShardBufferProcessorContainer(BufferGroupProcessorCreator<E, G, R> bufferProcessorCreator) {
        this.bufferProcessorCreator = bufferProcessorCreator;
    }

    public BufferGroupProcessor<E, G, R> get(SK shardKey) {
        BufferProcessorLazyer lazyer = bufferProcessorsMap.get(shardKey);
        return lazyer != null ? lazyer.get() : null;
    }

    public boolean isExist(SK shardKey) {
        return bufferProcessorsMap.containsKey(shardKey);
    }

    public boolean isNotExist(SK shardKey) {
        return !isExist(shardKey);
    }

    public void add(SK shardKey) {
        if (isExist(shardKey)) {
            return;
        }

        // 添加缓冲处理器的Lazyer到Map中
        bufferProcessorsMap.putIfAbsent(shardKey, new BufferProcessorLazyer());
    }

    public int getProcessorCount() {
        return bufferProcessorsMap.size();
    }

    private class BufferProcessorLazyer {

        private volatile BufferGroupProcessor<E, G, R> bufferProcessor;

        public BufferGroupProcessor<E, G, R> get() {
            if (bufferProcessor == null) {
                synchronized (this) {
                    if (bufferProcessor == null) {
                        bufferProcessor = bufferProcessorCreator.get();
                    }
                }
            }

            return bufferProcessor;
        }
    }

}
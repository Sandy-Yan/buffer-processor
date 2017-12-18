package com.github.andy.buffer.group;

/**
 * Created by yanshanguang on 17/12/11.
 */
public interface ShardBufferProcessorStrategy<E> {

    int routeProcessorIDX(int processorsCount, E element);
}

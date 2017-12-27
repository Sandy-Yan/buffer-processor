package com.github.andy.buffer.group;

/**
 * Created by yanshanguang on 17/12/8.
 */
public interface BufferGroupStrategy<E, G> {

    G doGroup(E element) throws Exception;
}

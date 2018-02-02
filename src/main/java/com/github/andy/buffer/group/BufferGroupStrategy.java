package com.github.andy.buffer.group;

/**
 * 缓冲处理器中消费实体分组的策略
 * <p>
 * Created by yanshanguang on 17/12/8.
 */
public interface BufferGroupStrategy<E, G> {

    G doGroup(final E element) throws Exception;
}

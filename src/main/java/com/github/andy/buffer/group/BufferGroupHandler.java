package com.github.andy.buffer.group;

import java.util.List;
import java.util.Map;

/**
 * 缓冲处理器中实体分组后真正逻辑处理
 * <p>
 * Created by yanshanguang on 17/12/8.
 */
public interface BufferGroupHandler<E, G, R> {

    Map<E, R> handle(final G group, final List<E> elements) throws Exception;
}
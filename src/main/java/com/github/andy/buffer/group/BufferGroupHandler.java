package com.github.andy.buffer.group;

import java.util.List;
import java.util.Map;

/**
 * Created by yanshanguang on 17/12/8.
 */
public interface BufferGroupHandler<E, R> {

    Map<E, R> handle(List<E> elements);
}

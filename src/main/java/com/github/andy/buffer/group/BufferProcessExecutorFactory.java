package com.github.andy.buffer.group;

import java.util.concurrent.ExecutorService;

/**
 * Created by yanshanguang on 17/12/12.
 */
public interface BufferProcessExecutorFactory {

    ExecutorService create();
}

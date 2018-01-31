package com.github.andy.buffer.group;

import java.util.concurrent.ExecutorService;

/**
 * 缓冲处理器中真正处理实体的线程池的构建工厂
 * <p>
 * Created by yanshanguang on 17/12/12.
 */
public interface BufferProcessExecutorFactory {

    ExecutorService create();
}

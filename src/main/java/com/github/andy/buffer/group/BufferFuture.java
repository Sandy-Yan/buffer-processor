package com.github.andy.buffer.group;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 缓冲处理Future
 * <p>
 * Created by yanshanguang on 17/12/8.
 */
public interface BufferFuture<R> {

    R get() throws InterruptedException, ExecutionException;

    R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;
}

package com.github.andy.buffer.group;

import java.util.concurrent.*;

/**
 * Created by yanshanguang on 17/12/8.
 */
public class BufferFutureTask<E, R> implements BufferFuture<R> {

    /**
     * 待处理的实体
     */
    private final E element;

    /**
     * 响应结果的Future
     */
    private final FutureTask<R> futureTask;

    /**
     * 最终返回的结果，通过state读写
     */
    private volatile Object result;

    public BufferFutureTask(E element) {
        this.element = element;
        this.futureTask = new FutureTask<R>(new Callable<R>() {
            @Override
            public R call() throws Exception {
                if (result instanceof Exception) {
                    throw (Exception) result;
                }
                return (R) result;
            }
        });
    }

    public E getElement() {
        return element;
    }

    @Override
    public R get() throws InterruptedException, ExecutionException {
        return futureTask.get();
    }

    @Override
    public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return futureTask.get(timeout, unit);
    }

    protected void completeSuccess(R result) {
        complete(result);
    }

    protected void completeFail(Exception ex) {
        complete(ex);
    }

    private void complete(Object result) {
        if (this.result == null) {
            synchronized (this) {
                if (this.result == null) {
                    this.result = result;
                    // 唤醒等待线程获取结果
                    futureTask.run();
                }
            }
        }
    }

}
package com.github.andy.buffer.group;

import com.github.andy.buffer.group.exception.GroupFailException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 实体缓冲分组处理器
 * <p>
 * Created by yanshanguang on 17/12/8.
 */
public class BufferGroupProcessor<E, G, R> {

    private final BlockingQueue<BufferFutureTask<E, R>> bufferQueue;

    private final int consumeBatchSize;

    private final int consumeWaitTimeoutMs;

    private final BufferGroupStrategy<E, G> bufferGroupStrategy;

    private final BufferGroupHandler<E, G, R> bufferGroupHandler;

    private final ExecutorService consumeExecutorService;

    private final ExecutorService processExecutorService;

    private final ReentrantLock lock = new ReentrantLock();

    private final AtomicBoolean running = new AtomicBoolean();

    public BufferGroupProcessor(int bufferQueueSize,
                                int consumeBatchSize,
                                int consumeWaitTimeoutMs,
                                BufferGroupStrategy<E, G> bufferGroupStrategy,
                                BufferGroupHandler<E, G, R> bufferGroupHandler,
                                ExecutorService processExecutorService) {
        this.bufferQueue = new LinkedBlockingQueue<>(bufferQueueSize);
        this.consumeBatchSize = consumeBatchSize;
        this.consumeWaitTimeoutMs = consumeWaitTimeoutMs;
        this.bufferGroupStrategy = bufferGroupStrategy;
        this.bufferGroupHandler = bufferGroupHandler;
        this.consumeExecutorService = Executors.newSingleThreadExecutor();
        this.processExecutorService = processExecutorService;
        init();
    }

    private void init() {
        startBatchConsume();
    }

    public static <E, G, R> BufferGroupProcessorBuilder<E, G, R> newBuilder() {
        return new BufferGroupProcessorBuilder<E, G, R>();
    }

    public BufferFuture<R> submit(E element) throws InterruptedException {
        if (element == null) {
            throw new NullPointerException();
        }

        BufferFutureTask<E, R> bufferFutureTask = newBufferFutureTask(element);
        bufferQueue.put(bufferFutureTask);
        return bufferFutureTask;
    }

    public long getQueueSize() {
        return bufferQueue.size();
    }

    public boolean isRunning() {
        return running.get();
    }

    private BufferFutureTask<E, R> newBufferFutureTask(E element) {
        return new BufferFutureTask<E, R>(element);
    }

    private void startBatchConsume() {
        consumeExecutorService.execute(new Runnable() {
            @Override
            public void run() {
                loopDoBatchConsume();
            }
        });
    }

    private void loopDoBatchConsume() {
        boolean isLoop = true;
        while (isLoop) {
            doBatchConsume();
        }
    }

    private void doBatchConsume() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            running.set(true);
            while (hasQueueFutureTasks()) {
                List<BufferFutureTask<E, R>> bufferFutureTasks = takeQueueFutureTasks();
                doConsumeFutureTasks(bufferFutureTasks);
            }
        } finally {
            running.set(false);
            lock.unlock();
        }
    }

    private boolean hasQueueFutureTasks() {
        return !bufferQueue.isEmpty();
    }

    private List<BufferFutureTask<E, R>> takeQueueFutureTasks() {
        List<BufferFutureTask<E, R>> futureTasks = new ArrayList<>(consumeBatchSize);
        try {
            Queues.drain(bufferQueue, futureTasks, consumeBatchSize, consumeWaitTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return futureTasks;
    }

    private void doConsumeFutureTasks(List<BufferFutureTask<E, R>> toConsumeFutureTasks) {
        if (toConsumeFutureTasks == null || toConsumeFutureTasks.isEmpty()) {
            return;
        }

        if (!isHasBufferGroupStrategy()) {
            doHandleGroupFutureTasks(null, toConsumeFutureTasks);
            return;
        }

        Map<G, List<BufferFutureTask<E, R>>> groupFutureTasksMap = doGroupFutureTasks(toConsumeFutureTasks);

        for (Map.Entry<G, List<BufferFutureTask<E, R>>> groupEntry : groupFutureTasksMap.entrySet()) {
            doHandleGroupFutureTasks(groupEntry.getKey(), groupEntry.getValue());
        }
    }

    private boolean isHasBufferGroupStrategy() {
        return bufferGroupStrategy != null;
    }

    private Map<G, List<BufferFutureTask<E, R>>> doGroupFutureTasks(List<BufferFutureTask<E, R>> toConsumeFutureTasks) {
        Map<G, List<BufferFutureTask<E, R>>> groupFutureTasksMap = Maps.newHashMap();
        List<BufferFutureTask<E, R>> groupFailFutureTasks = Lists.newArrayList();

        E element;
        G group;
        for (BufferFutureTask<E, R> futureTask : toConsumeFutureTasks) {
            element = futureTask.getElement();
            try {
                group = bufferGroupStrategy.doGroup(element);
            } catch (Exception ex) {
                groupFailFutureTasks.add(futureTask);
                continue;
            }

            // 添加task到分组列表
            List<BufferFutureTask<E, R>> groupList = groupFutureTasksMap.get(group);
            if (groupList == null) {
                groupList = Lists.newArrayList();
                groupFutureTasksMap.put(group, groupList);
            }

            groupList.add(futureTask);
        }

        // 完成分组失败异常响应
        completeGroupFailTasks(groupFailFutureTasks);

        return groupFutureTasksMap;
    }

    private void completeGroupFailTasks(List<BufferFutureTask<E, R>> groupFailFutureTasks) {
        completeFails(groupFailFutureTasks, new GroupFailException("[buffer-processor] 实体分组失败！请检查缓冲分组策略配置。"));
    }

    private void doHandleGroupFutureTasks(G group, List<BufferFutureTask<E, R>> futureTasks) {
        try {
            // 提交任务到执行分组对象处理的线程池
            processExecutorService.execute(new BufferGroupFuturesHandleTask(group, futureTasks));
        } catch (Exception ex) {
            // 提交任务失败处理，完成提交任务失败的异常响应
            completeFails(futureTasks, ex);
        }
    }

    private void completeFails(List<BufferFutureTask<E, R>> futureTasks, Exception ex) {
        if (futureTasks == null || futureTasks.isEmpty()) {
            return;
        }

        for (BufferFutureTask<E, R> futureTask : futureTasks) {
            futureTask.completeFail(ex);
        }
    }

    private class BufferGroupFuturesHandleTask implements Runnable {

        private final G group;

        private final List<BufferFutureTask<E, R>> bufferFutureTasks;

        public BufferGroupFuturesHandleTask(G group, List<BufferFutureTask<E, R>> bufferFutureTasks) {
            this.group = group;
            this.bufferFutureTasks = bufferFutureTasks;
        }

        @Override
        public void run() {

            // 准备待处理的分组对象
            List<E> elements = getTaskElements();

            // 执行分组对象处理
            Map<E, R> elementResultsMap;
            try {
                elementResultsMap = bufferGroupHandler.handle(group, elements);
            } catch (Exception ex) {
                // 完成分组对象处理异常结果响应
                completeFails(bufferFutureTasks, ex);
                return;
            }

            // 完成分组对象处理正常结果响应
            completeSuccessResults(elementResultsMap);
        }

        private List<E> getTaskElements() {
            List<E> elements = Lists.newArrayListWithCapacity(bufferFutureTasks.size());
            for (BufferFutureTask<E, R> bufferFutureTask : bufferFutureTasks) {
                elements.add(bufferFutureTask.getElement());
            }

            return elements;
        }

        private void completeSuccessResults(Map<E, R> elementResultsMap) {
            if (elementResultsMap != null && !elementResultsMap.isEmpty()) {
                // 完成响应Future结果为分组处理结果
                for (BufferFutureTask<E, R> bufferFutureTask : bufferFutureTasks) {
                    E element = bufferFutureTask.getElement();
                    R result = elementResultsMap.get(element);
                    bufferFutureTask.completeSuccess(result);
                }
            } else {
                // 完成响应Future结果为null
                for (BufferFutureTask<E, R> bufferFutureTask : bufferFutureTasks) {
                    bufferFutureTask.completeSuccess(null);
                }
            }
        }

    }

}
package com.github.andy.buffer.group;

import com.github.andy.buffer.group.exception.GroupFailException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 实体缓冲分组处理器
 * <p>
 * Created by yanshanguang on 17/12/8.
 */
public class BufferGroupProcessor<E, G, R> {

    private final BlockingQueue<BufferFutureTask<E, R>> bufferQueue;

    private final int consumeBatchSize;

    private final int maxConsumeIntervalSleepMs;

    private final BufferGroupStrategy<E, G> bufferGroupStrategy;

    private final ExecutorService consumeExecutorService;

    private final BufferGroupHandler<E, G, R> bufferGroupHandler;

    private final ExecutorService processExecutorService;

    private final ReentrantLock lock = new ReentrantLock();

    private final AtomicBoolean running = new AtomicBoolean();

    private final AtomicLong lastStartTimestamp = new AtomicLong(System.currentTimeMillis());

    public BufferGroupProcessor(int bufferQueueSize,
                                int consumeBatchSize,
                                int maxConsumeIntervalSleepMs,
                                BufferGroupStrategy<E, G> bufferGroupStrategy,
                                BufferGroupHandler<E, G, R> bufferGroupHandler,
                                ExecutorService processExecutorService) {
        this.bufferQueue = new LinkedBlockingQueue<>(bufferQueueSize);
        this.consumeBatchSize = consumeBatchSize;
        this.maxConsumeIntervalSleepMs = maxConsumeIntervalSleepMs;
        this.consumeExecutorService = Executors.newSingleThreadExecutor();
        this.bufferGroupStrategy = bufferGroupStrategy;
        this.bufferGroupHandler = bufferGroupHandler;
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
            while (!bufferQueue.isEmpty()) {
                trySleepBeforeConsume();
                List<BufferFutureTask<E, R>> bufferFutureTasks = new ArrayList<BufferFutureTask<E, R>>(Math.min(consumeBatchSize, bufferQueue.size()));
                bufferQueue.drainTo(bufferFutureTasks, consumeBatchSize);
                if (!bufferFutureTasks.isEmpty()) {
                    doConsume(bufferFutureTasks);
                }
            }
        } finally {
            running.set(false);
            lock.unlock();
        }
    }

    private void trySleepBeforeConsume() {
        long preStartTimestamp = lastStartTimestamp.get();
        lastStartTimestamp.set(System.currentTimeMillis());
        if (maxConsumeIntervalSleepMs > 0 && bufferQueue.size() < consumeBatchSize) {
            long maySleepMs = maxConsumeIntervalSleepMs - (System.currentTimeMillis() - preStartTimestamp);
            if (maySleepMs > 0L) {
                try {
                    TimeUnit.MILLISECONDS.sleep(Math.min(maySleepMs, maxConsumeIntervalSleepMs));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void doConsume(List<BufferFutureTask<E, R>> toConsumeBufferFutureTasks) {
        Map<G, List<BufferFutureTask<E, R>>> groupBufferFutureTasksMap = doGroupBufferFutureTasks(toConsumeBufferFutureTasks);
        for (Map.Entry<G, List<BufferFutureTask<E, R>>> groupBufferFutureTasksEntry : groupBufferFutureTasksMap.entrySet()) {
            doHandleGroupBufferFutureTasks(groupBufferFutureTasksEntry);
        }
    }

    private Map<G, List<BufferFutureTask<E, R>>> doGroupBufferFutureTasks(List<BufferFutureTask<E, R>> toConsumeBufferFutureTasks) {
        Map<G, List<BufferFutureTask<E, R>>> result = Maps.newHashMap();
        List<BufferFutureTask<E, R>> groupFailTasks = Lists.newArrayList();
        for (BufferFutureTask<E, R> bufferFutureTask : toConsumeBufferFutureTasks) {
            E element = bufferFutureTask.getElement();
            G group;

            try {
                group = bufferGroupStrategy.doGroup(element);
            } catch (Exception ex) {
                groupFailTasks.add(bufferFutureTask);
                continue;
            }

            // 添加task到分组列表
            List<BufferFutureTask<E, R>> groupList = result.get(group);
            if (groupList == null) {
                groupList = Lists.newArrayList();
                result.put(group, groupList);
            }

            groupList.add(bufferFutureTask);
        }

        // 完成分组失败异常响应
        completeGroupFailTasks(groupFailTasks);

        return result;
    }

    private void completeGroupFailTasks(List<BufferFutureTask<E, R>> groupFailTasks) {
        completeFails(groupFailTasks, new GroupFailException("实体分组失败！请检查缓冲分组策略配置。"));
    }

    private void doHandleGroupBufferFutureTasks(Map.Entry<G, List<BufferFutureTask<E, R>>> groupBufferFutureTasksEntry) {
        try {
            // 提交任务到执行分组对象处理的线程池
            processExecutorService.execute(new GroupBufferFutureTasksHandleTask(groupBufferFutureTasksEntry));
        } catch (Exception ex) {
            // 提交任务失败处理，完成提交任务失败的异常响应
            completeFails(groupBufferFutureTasksEntry.getValue(), ex);
        }
    }

    private void completeFails(List<BufferFutureTask<E, R>> bufferFutureTasks, Exception ex) {
        if (bufferFutureTasks == null || bufferFutureTasks.isEmpty()) {
            return;
        }

        for (BufferFutureTask<E, R> bufferFutureTask : bufferFutureTasks) {
            bufferFutureTask.completeFail(ex);
        }
    }

    private class GroupBufferFutureTasksHandleTask implements Runnable {

        private final G group;

        private final List<BufferFutureTask<E, R>> bufferFutureTasks;

        public GroupBufferFutureTasksHandleTask(Map.Entry<G, List<BufferFutureTask<E, R>>> groupBufferFutureTasksEntry) {
            this.group = groupBufferFutureTasksEntry.getKey();
            this.bufferFutureTasks = groupBufferFutureTasksEntry.getValue();
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
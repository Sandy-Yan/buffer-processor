package com.github.andy.buffer.group;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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

    private final BufferGroupStrategy<E, G> bufferGroupStrategy;

    private final ExecutorService consumeExecutorService;

    private final BufferGroupHandler<E, R> bufferGroupHandler;

    private final ExecutorService processExecutorService;

    private final ReentrantLock lock = new ReentrantLock();

    private final AtomicBoolean running = new AtomicBoolean();

    public BufferGroupProcessor(int bufferQueueSize,
                                int consumeBatchSize,
                                BufferGroupStrategy<E, G> bufferGroupStrategy,
                                BufferGroupHandler<E, R> bufferGroupHandler,
                                ExecutorService processExecutorService) {
        this.bufferQueue = new LinkedBlockingQueue<>(bufferQueueSize);
        this.consumeBatchSize = consumeBatchSize;
        this.consumeExecutorService = Executors.newSingleThreadExecutor();
        this.bufferGroupStrategy = bufferGroupStrategy;
        this.bufferGroupHandler = bufferGroupHandler;
        this.processExecutorService = processExecutorService;
        init();
    }

    private void init() {
        consumeExecutorService.execute(new Runnable() {
            @Override
            public void run() {
                loopDoBatchConsume();
            }
        });
    }

    public BufferFuture<R> submit(E element) throws InterruptedException {
        if (element == null) {
            throw new NullPointerException();
        }

        BufferFutureTask<E, R> bufferFutureTask = newBufferFutureTask(element);
        bufferQueue.put(bufferFutureTask);
        return bufferFutureTask;
    }

    private BufferFutureTask<E, R> newBufferFutureTask(E element) {
        return new BufferFutureTask<E, R>(element);
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

    private void doConsume(List<BufferFutureTask<E, R>> toConsumeBufferFutureTasks) {
        Map<G, List<BufferFutureTask<E, R>>> groupBufferFutureTasksMap = doGroupBufferFutureTasks(toConsumeBufferFutureTasks);
        for (Map.Entry<G, List<BufferFutureTask<E, R>>> groupBufferFutureTasksEntry : groupBufferFutureTasksMap.entrySet()) {
            doHandleGroupBufferFutureTasks(groupBufferFutureTasksEntry);
        }
    }

    private Map<G, List<BufferFutureTask<E, R>>> doGroupBufferFutureTasks(List<BufferFutureTask<E, R>> toConsumeBufferFutureTasks) {
        Map<G, List<BufferFutureTask<E, R>>> result = Maps.newHashMap();
        for (BufferFutureTask<E, R> bufferFutureTask : toConsumeBufferFutureTasks) {
            E element = bufferFutureTask.getElement();
            G group = bufferGroupStrategy.doGroup(element);

            List<BufferFutureTask<E, R>> groupList = result.get(group);
            if (groupList == null) {
                groupList = Lists.newArrayList();
                result.put(group, groupList);
            }

            groupList.add(bufferFutureTask);
        }
        return result;
    }

    private void doHandleGroupBufferFutureTasks(Map.Entry<G, List<BufferFutureTask<E, R>>> groupBufferFutureTasksEntry) {
        processExecutorService.execute(new GroupBufferFutureTasksHandleTask(groupBufferFutureTasksEntry));
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
            List<E> elements = Lists.newArrayListWithCapacity(bufferFutureTasks.size());
            for (BufferFutureTask<E, R> bufferFutureTask : bufferFutureTasks) {
                elements.add(bufferFutureTask.getElement());
            }

            // 执行分组对象处理
            Map<E, R> elementResultsMap = bufferGroupHandler.handle(elements);

            // 完成响应Future结果
            for (BufferFutureTask<E, R> bufferFutureTask : bufferFutureTasks) {
                E element = bufferFutureTask.getElement();
                R result = elementResultsMap.get(element);
                bufferFutureTask.complete(result);
            }
        }

    }

}

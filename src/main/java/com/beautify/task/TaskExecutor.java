package com.beautify.task;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.RateLimiter;
import redis.clients.jedis.JedisCommands;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskExecutor {
    private static Set<String> taskName = new HashSet<>();
    private Object condition = new Object();

    private TaskExecutor() {
    }

    /**
     * 任务名称
     */
    private String taskDesc;
    /**
     * 任务生产者
     */
    private TaskBatchProducer taskBatchProducer;

    /**
     * 配置
     */
    private ExecutorConfig executorConfig;
    /**
     * 任务缓冲队列
     */
    private volatile Queue<Task> queue;
    /**
     * 限流qps
     */
    private RateLimiter rateLimiter;

    private JedisCommands jedisCommands;

    /**
     * 0: 本地 1：分布式
     */
    private int mode;

    /**
     * 任务线程池
     */
    private ThreadPoolExecutor executorService;

    private volatile AtomicInteger concurrentModifyCount = new AtomicInteger();

    private static final int LOCAL = 0;
    private static final int DISTRIBUTED = 1;

    private static final int MAX_PARALLELIS = 100;

    public static TaskExecutorBuilder newBuild() {
        return new TaskExecutorBuilder();
    }

    public TaskExecutor start() {
        int parallelis = executorConfig.getParallelis();
        executorService = new ThreadPoolExecutor(parallelis, parallelis, 3L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>()
                , new ThreadFactory() {
            private AtomicInteger count = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(taskDesc + "-task-" + count.getAndIncrement());

                return thread;
            }
        });

        for (int index = 0; index < executorConfig.getParallelis(); index++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        checkConfig();

                        if (isStop()) {
                            sleep(10, TimeUnit.SECONDS);
                            continue;
                        }

                        if (isEmpty(queue) && taskBatchProducer == null) {
                            sleep(5, TimeUnit.MILLISECONDS);
                        }

                        if (isEmpty(queue) && taskBatchProducer != null) {
                            produceTask();
                        }

                        rateLimiter.acquire();

                        Task task = queue.poll();
                        if (task == null) {
                            continue;
                        }
                        try {
                            task.run();
                        } catch (Exception e) {
                            //
                        }
                    }
                }
            });
        }

        return this;
    }

    public void addTask(Task task) {
        queue.add(task);
    }

    private volatile long lastCheck = 0L;
    private static final int HALF_MINUTE = 30 * 1000;

    private void checkConfig() {
        //每隔30s检查一次，getCorePoolSize方法是阻塞的
        if (System.currentTimeMillis() - lastCheck <= HALF_MINUTE) {
            return;
        }

        lastCheck = System.currentTimeMillis();
        int parallelis = executorConfig.getParallelis();

        if (executorService.getCorePoolSize() != parallelis && parallelis > 0 && parallelis < MAX_PARALLELIS) {
            executorService.setCorePoolSize(parallelis);
        }

        double newRate = executorConfig.getRate();

        if (rateLimiter.getRate() != newRate && newRate > 0) {
            rateLimiter.setRate(newRate);
        }
    }

    private void produceTask() {
        if (mode == LOCAL) {
            createLocalTask();
        }

        if (mode == DISTRIBUTED) {
            createDistributeTask();
        }
    }

    private void createDistributeTask() {
        String ip = getLocalHost();

        String lockKey = buildLockKey();

        if (tryLock(lockKey, ip)) {
            createLocalTask();
        } else {
            sleep(5, TimeUnit.SECONDS);
        }
    }

    private static String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException var1) {
            return "";
        }
    }

    private void sleep(int sleep, TimeUnit timeUnit) {
        try {
            condition.wait(timeUnit.toMillis(sleep));
        } catch (InterruptedException e) {
            //do nothing
        }
    }

    /**
     * 生产任务
     *
     * @return 是否有任务被生产
     */
    private boolean produce() {
        List<Task> produce = null;
        while (produce == null || produce.size() == 0) {
            try {
                produce = taskBatchProducer.produce();
            } catch (Exception e) {
                sleep(10, TimeUnit.SECONDS);
                continue;
            }

            if (isStop()) {
                return false;
            }
            if (isEmpty(produce)) {
                sleep(executorConfig.sleepInMills(), TimeUnit.MILLISECONDS);
            }
        }

        if (!isEmpty(produce)) {
            queue.addAll(produce);
            return true;
        }
        return false;
    }

    /**
     * 保证任务只被一台机器生产
     *
     * @param key   锁名称
     * @param value 主机唯一标示：IP
     * @return 是否获取到锁
     */
    private boolean tryLock(String key, String value) {
        String statusCode = jedisCommands.set(key, value, "NX", "EX", TimeUnit.MINUTES.toSeconds(10));
        //加锁成功直接返回
        if (equalsIgnoreCase(statusCode, "OK")) {
            return true;
        }
        //如果是重复加锁，那么直接返回成功并延长过期时间
        String lockValue = jedisCommands.get(key);

        if (equals(lockValue, value)) {
            jedisCommands.expire(key, (int) TimeUnit.MINUTES.toSeconds(10));
            return true;
        }

        return false;
    }

    private String buildLockKey() {
        return "t_l_" + taskDesc;
    }

    private void createLocalTask() {
        int modify = concurrentModifyCount.intValue();
        synchronized (queue) {
            if (!concurrentModifyCount.compareAndSet(modify, modify)) {
                return;
            }
            if (produce()) {
                concurrentModifyCount.incrementAndGet();
            }
        }
    }

    public void stop() {
        executorService.shutdown();
    }

    private boolean isStop() {
        return executorConfig.getTaskStatus() != TaskStatus.RUNNING;
    }

    public static class TaskExecutorBuilder {
        /**
         * 任务生产者
         */
        private TaskBatchProducer taskBatchProducer;

        /**
         * 任务执行并行度
         */
        private ExecutorConfig executorConfig;

        private Queue<Task> queue;

        /**
         * 任务名称
         */
        private String taskDesc;

        private JedisCommands jedisCommands;

        /**
         * 0: 本地 1：分布式
         */
        private int mode;

        public BlockingQueueBuilder local(String taskDesc) {
            this.taskDesc = taskDesc;
            mode = LOCAL;
            return new BlockingQueueBuilder(this);
        }

        public RedisBuilder distributed(String taskDesc) {
            this.taskDesc = taskDesc;
            mode = DISTRIBUTED;
            return new RedisBuilder(this);
        }

        public class RedisBuilder extends TaskExecutorBuilder {
            private TaskExecutorBuilder taskExecutorBuilder;

            public RedisBuilder(TaskExecutorBuilder taskExecutorBuilder) {
                this.taskExecutorBuilder = taskExecutorBuilder;
            }

            public BlockingQueueBuilder redis(JedisCommands jedisCommands) {
                taskExecutorBuilder.jedisCommands = jedisCommands;
                return new BlockingQueueBuilder(this.taskExecutorBuilder);
            }
        }

        public class BlockingQueueBuilder extends TaskExecutorBuilder {
            private TaskExecutorBuilder taskExecutorBuilder;

            BlockingQueueBuilder(TaskExecutorBuilder taskExecutorBuilder) {
                this.taskExecutorBuilder = taskExecutorBuilder;
            }

            public ExecutorConfigBuilder queue(Queue<Task> queue) {
                taskExecutorBuilder.queue = queue;
                return new ExecutorConfigBuilder(this.taskExecutorBuilder);
            }
        }

        public class ExecutorConfigBuilder extends TaskExecutorBuilder {
            private TaskExecutorBuilder taskExecutorBuilder;

            ExecutorConfigBuilder(TaskExecutorBuilder taskExecutorBuilder) {
                this.taskExecutorBuilder = taskExecutorBuilder;
            }

            public BaseTaskExecutorBuilder config(ExecutorConfig executorConfig) {
                taskExecutorBuilder.executorConfig = executorConfig;
                return new BaseTaskExecutorBuilder(this.taskExecutorBuilder);
            }
        }

        public class BaseTaskExecutorBuilder extends TaskExecutorBuilder {
            private TaskExecutorBuilder taskExecutorBuilder;

            public BaseTaskExecutorBuilder(TaskExecutorBuilder taskExecutorBuilder) {
                this.taskExecutorBuilder = taskExecutorBuilder;
            }

            public BaseTaskExecutorBuilder taskBatchProducer(TaskBatchProducer taskBatchProducer) {
                this.taskExecutorBuilder.taskBatchProducer = taskBatchProducer;
                return new BaseTaskExecutorBuilder(this.taskExecutorBuilder);
            }

            public TaskExecutor start() {
                TaskExecutor taskExecutor = new TaskExecutor();

                taskExecutor.executorConfig = this.taskExecutorBuilder.executorConfig;
                taskExecutor.queue = this.taskExecutorBuilder.queue;
                taskExecutor.taskBatchProducer = this.taskExecutorBuilder.taskBatchProducer;
                taskExecutor.taskDesc = this.taskExecutorBuilder.taskDesc;
                taskExecutor.jedisCommands = this.taskExecutorBuilder.jedisCommands;
                taskExecutor.mode = this.taskExecutorBuilder.mode;
                taskExecutor.rateLimiter = RateLimiter.create(this.taskExecutorBuilder.executorConfig.getRate());

                if (taskExecutor.queue == null) {
                    taskExecutor.queue = Queues.newLinkedBlockingQueue();
                }

                if (taskExecutor.executorConfig.getParallelis() > MAX_PARALLELIS) {
                    throw new IllegalArgumentException("parallelis[" + executorConfig.getParallelis() + "]不能超过" + MAX_PARALLELIS + "，请检查是否合理");
                }

                if (taskExecutor.queue == null) {
                    throw new IllegalArgumentException("queue为null，请检查参数");
                }

//                if (taskExecutor.taskBatchProducer == null) {
//                    throw new IllegalArgumentException("taskBatchProducer为null，请检查参数");
//                }

                if (isEmpty(taskExecutor.taskDesc)) {
                    throw new IllegalArgumentException("taskDesc为null，请检查参数");
                }

                if (TaskExecutor.taskName.contains(taskExecutor.taskDesc)) {
                    throw new IllegalArgumentException("taskDesc[" + taskExecutor.taskDesc + "]已经被使用，请更换");
                }

                if (taskExecutor.mode == DISTRIBUTED && taskExecutor.jedisCommands == null) {
                    throw new IllegalArgumentException("分布式模式下，必须使用redis");
                }

                TaskExecutor.taskName.add(taskExecutor.taskDesc);
                taskExecutor.start();
                return taskExecutor;
            }
        }
    }

    private static boolean isEmpty(final Collection<?> coll) {
        return coll == null || coll.isEmpty();
    }

    private static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    private static boolean equalsIgnoreCase(String cs1, String cs2) {
        if (cs1 == cs2) {
            return true;
        }
        if (cs1 == null || cs2 == null) {
            return false;
        }
        if (cs1.length() != cs2.length()) {
            return false;
        }
        return equals(cs1.toUpperCase(), cs2.toUpperCase());
    }

    private static boolean equals(String cs1, String cs2) {
        if (cs1 == cs2) {
            return true;
        }
        if (cs1 == null || cs2 == null) {
            return false;
        }
        if (cs1.length() != cs2.length()) {
            return false;
        }

        return cs1.equals(cs2);
    }
}

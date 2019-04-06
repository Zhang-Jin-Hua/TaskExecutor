# 分布式生产者消费者实现
>* 可自由调整队列实现模型，从Java提供的内存队列，到Redis分布队列
>* 通过限流以及并发数的控制来动态调控消费者的消费速度
>* 采用pull的方式消费，充分考虑消费者的消费能力
>* 可自由扩展，提供了队列模型，限流，并发数，休眠时间等的扩展点

# 实例
## 本地队列

#### 简单任务定义
````java
public class TestTask implements Task, Serializable {
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + ":" + System.currentTimeMillis());
    }
}
````
#### 任务生产者
````java
    public class Producer implements TaskBatchProducer {
        @Override
        public List<Task> produce() {
            Task task = new TestTask();
            return Lists.newArrayList(task);
        }
    }
````
#### 任务构建
````java
    TaskExecutor taskExecutor = TaskExecutor.newBuild()
                .local("hello")
                .queue(new LinkedBlockingDeque<Task>())
                .config(executorConfig)
                .taskBatchProducer(new Producer())
                .start();
````

## 分布式队列
默认实现为Redis队列，可自行扩展
````java
        JedisCommands jedisCommands = new Jedis("localhost", 6379);

        TaskExecutor taskExecutor = TaskExecutor.newBuild()
                .distributed("distributed")
                .redis(jedisCommands)
                .queue(new RedisQueue<Task>("distributed", jedisCommands, beanSerializable))
                .config(executorConfig)
                .taskBatchProducer(new Producer())
                .start();
````
## 扩展点
### 自定义队列模型
继承java.util.Queue，并实现isEmpty(),poll(),add(),addAll()接口即可。
### 动态调整任务的参数（并行度，限流，休眠时间，任务启停）
````java
ExecutorConfig config = new ExecutorConfig() {
            @Override
            public TaskStatus getTaskStatus() {
                boolean isStart = commonConfig.getConfig("task.start", true);
                return isStart ? TaskStatus.RUNNING : TaskStatus.STOP;
            }

            @Override
            public int getSleepInMills() {
                return commonConfig.getConfig("task.sleep", (int) TimeUnit.MINUTES.toMillis(10));
            }

            @Override
            public int getParallelis() {
                return commonConfig.getConfig("task.parallelis", 1);
            }

            @Override
            public double getRate() {
                return commonConfig.getConfig("task.rate", 1f);
            }
        };
````

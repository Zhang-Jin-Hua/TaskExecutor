package com.beautify.task;


import com.google.common.collect.Lists;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;

import java.io.*;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for simple App.
 */
public class AppTest {
    @Test
    public void localQueueTest() throws InterruptedException {
        TaskExecutor taskExecutor = TaskExecutor.newBuild()
                .local("hello")
                .queue(new LinkedBlockingDeque<Task>())
                .config(executorConfig)
                .taskBatchProducer(new Producer())
                .start();

        Thread.sleep(5000);
        Producer producer = new Producer();
        while (true) {
            List<Task> produce = producer.produce();
            for (Task task : produce) {
                taskExecutor.addTask(task);
            }
        }
    }

    @Test
    public void redisTest() throws InterruptedException {
        JedisCommands jedisCommands = new Jedis("localhost", 6379);

        TaskExecutor taskExecutor = TaskExecutor.newBuild()
                .distributed("distributed")
                .redis(jedisCommands)
                .queue(new RedisQueue<Task>("distributed", jedisCommands, beanSerializable))
                .config(executorConfig)
                .taskBatchProducer(new Producer())
                .start();

        Thread.sleep(5000);
        Producer producer = new Producer();
        while (true) {
            List<Task> produce = producer.produce();
            for (Task task : produce) {
                taskExecutor.addTask(task);
            }
        }
    }

    BeanSerializable<Task> beanSerializable = new BeanSerializable<Task>() {
        @Override
        public String serialize(Task task) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try {
                ObjectOutputStream oo = new ObjectOutputStream(byteArrayOutputStream);
                oo.writeObject(task);
                return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }

        @Override
        public Task deSerialize(String str) {
            try {
                ByteArrayInputStream inputStream = new ByteArrayInputStream(Base64.getDecoder().decode(str));
                ObjectInputStream ois = new ObjectInputStream(inputStream);
                Task task = (Task) ois.readObject();
                return task;
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            return null;
        }
    };


    public class Producer implements TaskBatchProducer {
        @Override
        public List<Task> produce() {
            Task task = new TestTask();
            return Lists.newArrayList(task);
        }
    }

    private static ExecutorConfig executorConfig = new ExecutorConfig() {
        @Override
        public int getParallelis() {
            return 2;
        }

        @Override
        public double getRate() {
            return 2;
        }

        @Override
        public TaskStatus getTaskStatus() {
            return TaskStatus.RUNNING;
        }

        @Override
        public int sleepInMills() {
            return (int) TimeUnit.SECONDS.toMillis(5);
        }
    };

}

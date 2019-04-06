package com.beautify.task;


import java.util.List;

/**
 * 任务生产者，单线程调用
 */
public interface TaskBatchProducer {
    List<Task> produce();
}

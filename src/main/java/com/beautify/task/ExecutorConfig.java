package com.beautify.task;


public interface ExecutorConfig {
    int getParallelis();

    double getRate();

    TaskStatus getTaskStatus();

    /**
     * 没有任务时候的休眠时间
     */
    int sleepInMills();
}

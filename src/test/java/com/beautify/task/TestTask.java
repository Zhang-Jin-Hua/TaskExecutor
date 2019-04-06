package com.beautify.task;

import java.io.Serializable;

public class TestTask implements Task, Serializable {
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + ":" + System.currentTimeMillis());
    }
}
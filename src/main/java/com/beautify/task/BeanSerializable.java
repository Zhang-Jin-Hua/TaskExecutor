package com.beautify.task;


public interface BeanSerializable<T> {
    String serialize(T t);

    T deSerialize(String str);
}

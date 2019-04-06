package com.beautify.task;

import redis.clients.jedis.JedisCommands;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class RedisQueue<E> implements Queue<E> {
    private static final String PREFIX = "R_Q_";
    private String queueName;
    private JedisCommands jedisCommands;
    private BeanSerializable<E> serializable;

    public RedisQueue(String queueName, JedisCommands jedisCommands, BeanSerializable<E> serializable) {
        this.queueName = queueName;
        this.jedisCommands = jedisCommands;
        this.serializable = serializable;
    }

    private String buildKey(String queueName) {
        return PREFIX + queueName;
    }

    @Override
    public int size() {
        Long llen = jedisCommands.llen(buildKey(queueName));
        return llen == null ? 0 : llen.intValue();
    }

    @Override
    public boolean isEmpty() {
        return !jedisCommands.exists(queueName);
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray(Object[] a) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        for (E e : c) {
            add(e);
        }
        return true;
    }


    @Override
    public void clear() {
        jedisCommands.del(queueName);
    }

    @Override
    public boolean retainAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(Object o) {
        return false;
    }

    @Override
    public boolean add(E e) {
        String jsonString = serializable.serialize(e);
        jedisCommands.lpush(queueName, jsonString);
        jedisCommands.expire(queueName, (int) TimeUnit.DAYS.toSeconds(5));

        return true;
    }

    @Override
    public E remove() {
        String lpop = jedisCommands.lpop(queueName);
        if (isEmpty(lpop)) {
            throw new NoSuchElementException();
        }

        return serializable.deSerialize(lpop);
    }

    @Override
    public E poll() {
        String lpop = jedisCommands.lpop(queueName);
        if (isEmpty(lpop)) {
            return null;
        }

        return serializable.deSerialize(lpop);
    }

    @Override
    public E element() {
        String lindex = jedisCommands.lindex(queueName, 0);
        if (isEmpty(lindex)) {
            throw new NoSuchElementException();
        }

        return serializable.deSerialize(lindex);
    }

    @Override
    public E peek() {
        String lindex = jedisCommands.lindex(queueName, 0);
        if (isEmpty(lindex)) {
            return null;
        }

        return serializable.deSerialize(lindex);
    }


    public static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

}

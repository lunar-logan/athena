package org.athena.service;

public interface CounterService {
    void createCounter(String name, long start, long step, long max);

    void deleteCounter(String name);

    long next(String counter);
}

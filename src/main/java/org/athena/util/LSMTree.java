package org.athena.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LSMTree implements Map<String, String> {
    private final SortedMap<String, String> m = new ConcurrentSkipListMap<>();

    @Override
    public int size() {
        return m.size();
    }

    @Override
    public boolean isEmpty() {
        return m.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return m.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return m.containsValue(value);
    }

    @Override
    public String get(Object key) {
        return m.get(key);
    }

    @Override
    public String put(String key, String value) {

        return m.put(key, value);
    }

    @Override
    public String remove(Object key) {
        return m.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        this.m.putAll(m);
    }

    @Override
    public void clear() {
        m.clear();
    }

    @Override
    public Set<String> keySet() {
        return m.keySet();
    }

    @Override
    public Collection<String> values() {
        return m.values();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return m.entrySet();
    }
}

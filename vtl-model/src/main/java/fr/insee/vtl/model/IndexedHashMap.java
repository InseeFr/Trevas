package fr.insee.vtl.model;

import java.util.*;

public class IndexedHashMap<K, V> implements Map<K, V> {

    private final Map<K, Integer> indices;
    private final Map<K, V> delegate;

    public IndexedHashMap() {
        this.indices = new HashMap<>();
        this.delegate = new HashMap<>();
    }

    public IndexedHashMap(int initialCapacity) {
        this.indices = new HashMap<>(initialCapacity);
        this.delegate = new LinkedHashMap<>(initialCapacity);
    }


    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return containsValue(value);
    }

    @Override
    public V get(Object key) {
        return delegate.get(key);
    }

    public int indexOfKey(K key) {
        return indices.getOrDefault(key, -1);
    }

    public int indexOfValue(V value) {
        if (value == null) {
            for (Entry<K, V> e : entrySet()) {
                if (e.getValue() == null) {
                    return indexOfKey(e.getKey());
                }
            }
        } else {
            for (Entry<K, V> e : entrySet()) {
                if (value.equals(e.getValue())) {
                    return indexOfKey(e.getKey());
                }
            }
        }
        return -1;
    }

    @Override
    public synchronized V put(K key, V value) {
        Integer index = delegate.containsKey(key)
                ? indices.get(key)
                : indices.values().stream().max(Integer::compareTo).orElse(-1) + 1;
        indices.put(key, index);
        return delegate.put(key, value);
    }

    @Override
    public V remove(Object key) {
        indices.remove(key);
        return delegate.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        indices.clear();
        delegate.clear();
    }

    @Override
    public Set<K> keySet() {
        return delegate.keySet();
    }

    @Override
    public Collection<V> values() {
        return delegate.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return delegate.entrySet();
    }
}

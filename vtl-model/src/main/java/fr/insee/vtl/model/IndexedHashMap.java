package fr.insee.vtl.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The <code>DoubleExpression</code> class is a delegated LinkedHashMap that has indexOf methods.
 */
public class IndexedHashMap<K, V> implements Map<K, V> {

  private final Map<K, Integer> indices;
  private final Map<K, V> delegate;

  /** Anonymous constructor. */
  public IndexedHashMap() {
    this.indices = new HashMap<>();
    this.delegate = new LinkedHashMap<>();
  }

  /**
   * Constructor specifying an initial capacity for the map.
   *
   * @param initialCapacity The initial capacity to use.
   */
  public IndexedHashMap(int initialCapacity) {
    this.indices = new HashMap<>(initialCapacity);
    this.delegate = new LinkedHashMap<>(initialCapacity);
  }

  /**
   * Constructor taking an existing map.
   *
   * @param map The existing map.
   */
  public IndexedHashMap(IndexedHashMap<K, V> map) {
    this.indices = new HashMap<>(map.indices);
    this.delegate = new LinkedHashMap<>(map.delegate);
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
    return delegate.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return delegate.get(key);
  }

  /**
   * Returns the index of a given key in the map, or -1 if the key is not present.
   *
   * @param key The key to look for.
   * @return The index of the key in the map, or -1 if the key is not present.
   */
  public int indexOfKey(K key) {
    return indices.getOrDefault(key, -1);
  }

  /**
   * The index of the first occurrence of a given value in the map, or -1 if the value is not
   * present.
   *
   * @param value The value to look for.
   * @return The index of the first occurrence of the value in the map, or -1 if the value is not
   *     present.
   */
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
    Integer index =
        delegate.containsKey(key)
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexedHashMap<?, ?> that = (IndexedHashMap<?, ?>) o;
    return this.delegate.equals(that.delegate);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }
}

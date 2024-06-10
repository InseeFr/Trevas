package fr.insee.vtl.model.utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Java8Helpers {

    public static class MapEntry<K, V> {

        private final K key;
        private final V value;

        public MapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public static <K, V> MapEntry<K, V> of(K key, V value) {
            return new MapEntry<>(key, value);
        }
    }

    @SafeVarargs // only read access
    public static <T> List<T> listOf(T... items) {
        List<T> tmpList = new ArrayList<>();
        Collections.addAll(tmpList, items);
        return Collections.unmodifiableList(tmpList);
    }

    @SafeVarargs // only read access
    public static <T> Set<T> setOf(T... items) {
        Set<T> tmpSet = new HashSet<>();
        Collections.addAll(tmpSet, items);
        return Collections.unmodifiableSet(tmpSet);
    }

    public static <K, V> Map<K, V> mapOf() {
        return Collections.unmodifiableMap(new HashMap<>());
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1) {
        Map<K, V> tmpMap = new HashMap<>();
        tmpMap.put(k1, v1);
        return Collections.unmodifiableMap(tmpMap);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2) {
        Map<K, V> tmpMap = new HashMap<>();
        tmpMap.put(k1, v1);
        tmpMap.put(k2, v2);
        return Collections.unmodifiableMap(tmpMap);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3) {
        Map<K, V> tmpMap = new HashMap<>();
        tmpMap.put(k1, v1);
        tmpMap.put(k2, v2);
        tmpMap.put(k3, v3);
        return Collections.unmodifiableMap(tmpMap);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        Map<K, V> tmpMap = new HashMap<>();
        tmpMap.put(k1, v1);
        tmpMap.put(k2, v2);
        tmpMap.put(k3, v3);
        tmpMap.put(k4, v4);
        return Collections.unmodifiableMap(tmpMap);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
        Map<K, V> tmpMap = new HashMap<>();
        tmpMap.put(k1, v1);
        tmpMap.put(k2, v2);
        tmpMap.put(k3, v3);
        tmpMap.put(k4, v4);
        tmpMap.put(k5, v5);
        return Collections.unmodifiableMap(tmpMap);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6) {
        Map<K, V> tmpMap = new HashMap<>();
        tmpMap.put(k1, v1);
        tmpMap.put(k2, v2);
        tmpMap.put(k3, v3);
        tmpMap.put(k4, v4);
        tmpMap.put(k5, v5);
        tmpMap.put(k6, v6);
        return Collections.unmodifiableMap(tmpMap);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7) {
        Map<K, V> tmpMap = new HashMap<>();
        tmpMap.put(k1, v1);
        tmpMap.put(k2, v2);
        tmpMap.put(k3, v3);
        tmpMap.put(k4, v4);
        tmpMap.put(k5, v5);
        tmpMap.put(k6, v6);
        tmpMap.put(k7, v7);
        return Collections.unmodifiableMap(tmpMap);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7, K k8, V v8) {
        Map<K, V> tmpMap = new HashMap<>();
        tmpMap.put(k1, v1);
        tmpMap.put(k2, v2);
        tmpMap.put(k3, v3);
        tmpMap.put(k4, v4);
        tmpMap.put(k5, v5);
        tmpMap.put(k6, v6);
        tmpMap.put(k7, v7);
        tmpMap.put(k8, v8);
        return Collections.unmodifiableMap(tmpMap);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7, K k8, V v8, K k9, V v9) {
        Map<K, V> tmpMap = new HashMap<>();
        tmpMap.put(k1, v1);
        tmpMap.put(k2, v2);
        tmpMap.put(k3, v3);
        tmpMap.put(k4, v4);
        tmpMap.put(k5, v5);
        tmpMap.put(k6, v6);
        tmpMap.put(k7, v7);
        tmpMap.put(k8, v8);
        tmpMap.put(k9, v9);
        return Collections.unmodifiableMap(tmpMap);
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7, K k8, V v8, K k9, V v9, K k10, V v10) {
        Map<K, V> tmpMap = new HashMap<>();
        tmpMap.put(k1, v1);
        tmpMap.put(k2, v2);
        tmpMap.put(k3, v3);
        tmpMap.put(k4, v4);
        tmpMap.put(k5, v5);
        tmpMap.put(k6, v6);
        tmpMap.put(k7, v7);
        tmpMap.put(k8, v8);
        tmpMap.put(k9, v9);
        tmpMap.put(k10, v10);
        return Collections.unmodifiableMap(tmpMap);
    }

    @SafeVarargs // only read access
    public static <K, V> Map<K, V> mapOfEntries(MapEntry<K, V>... entries) {
        Map<K, V> tmpMap = new HashMap<>();
        Arrays.stream(entries).forEach(entry -> tmpMap.put(entry.key, entry.value));
        return Collections.unmodifiableMap(tmpMap);
    }

    public static <T> Stream<T> streamIterator(Iterator<T> iterator) {
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false
        );
    }

    public static byte[] readAllBytes(InputStream inputStream) throws IOException {
        byte[] bytes = new byte[inputStream.available()];
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        dataInputStream.readFully(bytes);
        return bytes;
    }
}
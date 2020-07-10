package fr.insee.vtl.model;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class InMemoryDataset implements Dataset {

    private final List<Map<String, Object>> delegate;

    public InMemoryDataset(List<Map<String, Object>> delegate) {
        this.delegate = delegate;
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
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    @Override
    public Iterator<Map<String, Object>> iterator() {
        return delegate.iterator();
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return delegate.toArray(a);
    }

    @Override
    public boolean add(Map<String, Object> map) {
        return delegate.add(map);
    }

    @Override
    public boolean remove(Object o) {
        return delegate.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends Map<String, Object>> c) {
        return delegate.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends Map<String, Object>> c) {
        return delegate.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return delegate.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return delegate.retainAll(c);
    }

    @Override
    public void replaceAll(UnaryOperator<Map<String, Object>> operator) {
        delegate.replaceAll(operator);
    }

    @Override
    public void sort(Comparator<? super Map<String, Object>> c) {
        delegate.sort(c);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public Map<String, Object> get(int index) {
        return delegate.get(index);
    }

    @Override
    public Map<String, Object> set(int index, Map<String, Object> element) {
        return delegate.set(index, element);
    }

    @Override
    public void add(int index, Map<String, Object> element) {
        delegate.add(index, element);
    }

    @Override
    public Map<String, Object> remove(int index) {
        return delegate.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return delegate.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return delegate.lastIndexOf(o);
    }

    @Override
    public ListIterator<Map<String, Object>> listIterator() {
        return delegate.listIterator();
    }

    @Override
    public ListIterator<Map<String, Object>> listIterator(int index) {
        return delegate.listIterator(index);
    }

    @Override
    public List<Map<String, Object>> subList(int fromIndex, int toIndex) {
        return delegate.subList(fromIndex, toIndex);
    }

    @Override
    public Spliterator<Map<String, Object>> spliterator() {
        return delegate.spliterator();
    }

    @Override
    public <T> T[] toArray(IntFunction<T[]> generator) {
        return delegate.toArray(generator);
    }

    @Override
    public boolean removeIf(Predicate<? super Map<String, Object>> filter) {
        return delegate.removeIf(filter);
    }

    @Override
    public Stream<Map<String, Object>> stream() {
        return delegate.stream();
    }

    @Override
    public Stream<Map<String, Object>> parallelStream() {
        return delegate.parallelStream();
    }

    @Override
    public void forEach(Consumer<? super Map<String, Object>> action) {
        delegate.forEach(action);
    }
}

package fr.insee.vtl.spark;

import org.apache.spark.sql.Row;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The <code>SparkRowMap</code> class represents a row in a Spark dataset as a map.
 */
class SparkRowMap implements Map<String, Object> {

    private final Row row;

    /**
     * Constructor taking a Spark {@link Row}.
     *
     * @param row the row of the Spark dataset.
     */
    public SparkRowMap(Row row) {
        this.row = row;
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
        return row.fieldIndex((String) key) > -1;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(Object key) {
        return row.get(row.fieldIndex((String) key));
    }

    @Override
    public Object put(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Object> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        throw new UnsupportedOperationException();
    }
}

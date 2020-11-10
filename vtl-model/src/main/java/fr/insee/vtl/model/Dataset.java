package fr.insee.vtl.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <code>Dataset</code> is the base interface for structured datasets conforming to the VTL data model.
 */
public interface Dataset extends Structured {

    /**
     * Converts a dataset represented as a list of column values to a row-major order dataset in a specified column order.
     *
     * @param map     The input dataset represented as a <code>Map</code> of column names to column contents (<code>Object</code> instances).
     * @param columns A <code>List</code> of column names giving the order of the column contents in the returned list.
     * @return A <code>List</code> of column contents of the input dataset ordered as specified by the input list of column names.
     */
    static List<Object> mapToRowMajor(Map<String, Object> map, List<String> columns) {
        List<Object> row = new ArrayList<>(columns.size());
        while (row.size() < columns.size()) {
            row.add(null);
        }
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String column = entry.getKey();
            row.set(columns.indexOf(column), map.get(column));
        }
        return row;
    }

    /**
     * Returns the data contained in the dataset as a list of list of objects.
     *
     * @return The data contained in the dataset as a list of list of objects.
     */
    List<DataPoint> getDataPoints();

    default List<List<Object>> getDataAsList() {
        return getDataPoints().stream().map(objects -> (List<Object>) new ArrayList<>(objects)).collect(Collectors.toList());
    }

    /**
     * Returns the data contained in the dataset as a list of mappings between column names and column contents.
     *
     * @return The data contained in the dataset as a list of mappings between column names and column contents.
     */
    default List<Map<String, Object>> getDataAsMap() {
        return getDataPoints().stream().map(DataPointMap::new).collect(Collectors.toList());
    }

    /**
     * The <code>Role</code> <code>Enumeration</code> lists the roles of a component in a dataset structure.
     */
    enum Role {
        /**
         * The component is an identifier in the data structure
         */
        IDENTIFIER,
        /**
         * The component is a measure in the data structure
         */
        MEASURE,
        /**
         * The component is an attribute in the data structure
         */
        ATTRIBUTE
    }
}

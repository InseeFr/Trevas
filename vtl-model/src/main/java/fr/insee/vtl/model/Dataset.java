package fr.insee.vtl.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    List<List<Object>> getDataPoints();

    /**
     * Returns the data contained in the dataset as a list of mappings between column names and column contents.
     *
     * @return The data contained in the dataset as a list of mappings between column names and column contents.
     */
    default List<Map<String, Object>> getDataAsMap() {
        return getDataPoints().stream().map(objects ->
                IntStream.range(0, getDataStructure().size())
                        .boxed()
                        .collect(Collectors.toMap(idx -> getDataStructure().get(idx).getName(), objects::get))
        ).collect(Collectors.toList());
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

    /**
     * The <code>Structure</code> class represent a structure component with its name, type and role.
     */
    class Component {

        private final String name;
        private final Class<?> type;
        private final Role role;

        /**
         * Constructor taking the name, type and role of the component.
         *
         * @param name A string giving the name of the structure component to create.
         * @param type A <code>Class</code> giving the type of the structure component to create.
         * @param role A <code>Role</code> giving the role of the structure component to create.
         */
        public Component(String name, Class<?> type, Role role) {
            this.name = Objects.requireNonNull(name);
            this.type = Objects.requireNonNull(type);
            this.role = Objects.requireNonNull(role);
        }

        /**
         * Returns the name of the component.
         *
         * @return The name of the component as a string.
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the type of the component.
         *
         * @return The type of the component as an instance of <code>Class</code>.
         */
        public Class<?> getType() {
            return type;
        }

        /**
         * Returns the role of component.
         *
         * @return The role of the component as a value of the <code>Role</code> enumeration.
         */
        public Role getRole() {
            return role;
        }
    }
}

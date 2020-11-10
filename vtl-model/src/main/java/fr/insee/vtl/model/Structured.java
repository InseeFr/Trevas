package fr.insee.vtl.model;

import java.util.*;
import java.util.stream.Collectors;

/**
 * <code>Structured</code> is the base interface for representing structured data.
 */
public interface Structured {

    /**
     * Returns the structure associated to the data as a list of structure components.
     *
     * @return The structure associated to the data as a list of structure components.
     */
    DataStructure getDataStructure();

    /**
     * Returns the list of column names.
     *
     * @return The column names as a list of strings.
     */
    default List<String> getColumnNames() {
        return new ArrayList<>(getDataStructure().keySet());
    }

    /**
     * The <code>Structure</code> class represent a structure component with its name, type and role.
     */
    class Component {

        private final String name;
        private final Class<?> type;
        private final Dataset.Role role;

        /**
         * Constructor taking the name, type and role of the component.
         *
         * @param name A string giving the name of the structure component to create.
         * @param type A <code>Class</code> giving the type of the structure component to create.
         * @param role A <code>Role</code> giving the role of the structure component to create.
         */
        public Component(String name, Class<?> type, Dataset.Role role) {
            this.name = Objects.requireNonNull(name);
            this.type = Objects.requireNonNull(type);
            this.role = Objects.requireNonNull(role);
        }

        public Component(Component component) {
            this.name = component.getName();
            this.type = component.getType();
            this.role = component.getRole();
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
        public Dataset.Role getRole() {
            return role;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Component component = (Component) o;
            return name.equals(component.name) &&
                    type.equals(component.type) &&
                    role == component.role;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, role);
        }

        @Override
        public String toString() {
            return "Component{" + name +
                    ", type=" + type +
                    ", role=" + role +
                    '}';
        }
    }

    class DataStructure extends IndexedHashMap<String, Structured.Component> {

        public DataStructure(Map<String, Class<?>> types, Map<String, Dataset.Role> roles) {
            super(types.size());
            if (!types.keySet().equals(roles.keySet())) {
                throw new IllegalArgumentException("type and roles key sets inconsistent");
            }
            for (String column : types.keySet()) {
                Component component = new Component(column, types.get(column), roles.get(column));
                put(column, component);
            }
        }

        public DataStructure(Collection<Component> components) {
            super(components.size());
            for (Component component : components) {
                var newComponent = new Component(component);
                Component old = put(newComponent.getName(), newComponent);
                if (old != null) {
                    throw new IllegalArgumentException("duplicate column in " + components);
                }
            }
        }

        public DataStructure(DataStructure dataStructure) {
            super(dataStructure);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataStructure objects = (DataStructure) o;
            // TODO: Optimize
            return Objects.equals(new HashSet<>(values()), new HashSet<>(objects.values()));
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(values());
        }
    }

    class DataPoint extends ArrayList<Object> {

        private final DataStructure dataStructure;

        public DataPoint(DataStructure dataStructure, Map<String, Object> map) {
            super();
            growSize(dataStructure.size());
            this.dataStructure = Objects.requireNonNull(dataStructure);
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                this.set(entry.getKey(), entry.getValue());
            }
        }

        public DataPoint(DataStructure dataStructure) {
            super();
            growSize(dataStructure.size());
            this.dataStructure = Objects.requireNonNull(dataStructure);
        }

        public DataPoint(DataStructure dataStructure, Collection<Object> collection) {
            super(dataStructure.size());
            this.dataStructure = Objects.requireNonNull(dataStructure);
            addAll(collection);
        }

        private void growSize(int size) {
            while (size() < size) {
                add(null);
            }
        }

        public Object get(String column) {
            return get(dataStructure.indexOfKey(column));
        }

        public Object set(String column, Object object) {
            int index = dataStructure.indexOfKey(column);
            if (index > size() - 1) {
                growSize(index + 1);
            }
            return set(index, object);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataPoint objects = (DataPoint) o;
            for (Component component : dataStructure.values()) {
                if (!Dataset.Role.IDENTIFIER.equals(component.getRole())) {
                    continue;
                }
                if (!get(component.getName()).equals(objects.get(component.getName()))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hashCode = 1;
            for (Component component : dataStructure.values()) {
                if (!Dataset.Role.IDENTIFIER.equals(component.getRole())) {
                    continue;
                }
                Object e = get(component.getName());
                hashCode = 31 * hashCode + (e == null ? 0 : e.hashCode());
            }
            return hashCode;
        }
    }

    class DataPointMap implements Map<String, Object> {

        private final DataPoint dataPoint;

        public DataPointMap(DataPoint dataPoint) {
            this.dataPoint = dataPoint;
        }

        @Override
        public int size() {
            return dataPoint.size();
        }

        @Override
        public boolean isEmpty() {
            return dataPoint.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(Object key) {
            return dataPoint.get((String) key);
        }

        @Override
        public Object put(String key, Object value) {
            return dataPoint.set(key, value);
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
            return dataPoint.dataStructure.keySet();
        }

        @Override
        public Collection<Object> values() {
            return dataPoint;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return dataPoint.dataStructure.keySet().stream()
                    .map(component -> new AbstractMap.SimpleEntry<>(
                            component,
                            dataPoint.get(component))
                    )
                    .collect(Collectors.toSet());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Map)) return false;
            Map<?, ?> that = (Map<?, ?>) o;
            return entrySet().equals(that.entrySet());
        }

        @Override
        public int hashCode() {
            return Objects.hash(entrySet());
        }
    }

}

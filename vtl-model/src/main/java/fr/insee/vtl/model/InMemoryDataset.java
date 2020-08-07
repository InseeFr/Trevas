package fr.insee.vtl.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Basic implementation of <code>Dataset</code> as an in-memory dataset.
 */
public class InMemoryDataset implements Dataset {

    private final List<List<Object>> data;
    private final List<Component> components;
    private final List<String> columns;

    /**
     * Constructor taking initial data and structure components types and roles.
     *
     * @param data The initial data as a list of mappings between column names and column contents.
     * @param types A mapping between the names and types of the columns as structure components.
     * @param roles A mapping between the names and roles of the columns as structure components.
     */
    public InMemoryDataset(List<Map<String, Object>> data, Map<String, Class<?>> types, Map<String, Role> roles) {
        if (!Objects.requireNonNull(types).keySet().equals(Objects.requireNonNull(roles).keySet())) {
            throw new IllegalArgumentException("types and role keys differ");
        }
        this.columns = new ArrayList<>(types.keySet());
        this.data = Objects.requireNonNull(data).stream().map(map -> Dataset.mapToRowMajor(map, columns))
                .collect(Collectors.toList());
        this.components = new ArrayList<>(data.size());
        while (components.size() < columns.size()) {
            components.add(null);
        }
        for (String column : columns) {
            this.components.set(
                    columns.indexOf(column),
                    new Component(column, types.get(column), roles.get(column))
            );
        }
    }

    /**
     * Constructor taking initial data and a list of structure components.
     *
     * @param data The initial data as a list of list of objects representing data contents.
     * @param structures The list of structure components forming the structure of the dataset.
     */
    public InMemoryDataset(List<List<Object>> data, List<Component> structures) {
        this.components = Objects.requireNonNull(structures);
        this.columns = this.components.stream().map(Component::getName).collect(Collectors.toList());
        this.data = Objects.requireNonNull(data);
    }

    /**
     * Constructor taking structure components types and roles and creating an corresponding empty dataset.
     *
     * @param types A mapping between the names and types of the structure components.
     * @param roles A mapping between the names and roles of the structure components.
     */
    public InMemoryDataset(Map<String, Class<?>> types, Map<String, Role> roles) {
        this(new ArrayList<>(), types, roles);
    }

    @Override
    public List<List<Object>> getDataPoints() {
        return data;
    }

    @Override
    public List<Component> getDataStructure() {
        return components;
    }
}

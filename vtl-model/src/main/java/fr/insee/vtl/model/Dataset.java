package fr.insee.vtl.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface Dataset extends Structured {

    public static List<Object> mapToRowMajor(Map<String, Object> map, List<String> columns) {
        ArrayList<Object> datum = new ArrayList<>(columns.size());
        while (datum.size() < columns.size()) {
            datum.add(null);
        }
        for (Map.Entry<String,Object> entry : map.entrySet()) {
            String column = entry.getKey();
            datum.set(columns.indexOf(column), map.get(column));
        }
        return datum;
    }

    List<List<Object>> getDataPoints();

    default List<Map<String, Object>> getDataAsMap() {
        return getDataPoints().stream().map(objects ->
                IntStream.range(0, getDataStructure().size())
                        .boxed()
                        .collect(Collectors.toMap(idx -> getDataStructure().get(idx).getName(), objects::get))
        ).collect(Collectors.toList());
    }

    default List<String> getColumns() {
        return getDataStructure().stream().map(Structure::getName).collect(Collectors.toList());
    }

    enum Role {
        IDENTIFIER,
        MEASURE,
        ATTRIBUTE
    }

    class Structure {
        private final String name;
        private final Class<?> type;
        private final Role role;

        public Structure(String name, Class<?> type, Role role) {
            this.name = Objects.requireNonNull(name);
            this.type = Objects.requireNonNull(type);
            this.role = Objects.requireNonNull(role);
        }

        public String getName() {
            return name;
        }

        public Class<?> getType() {
            return type;
        }

        public Role getRole() {
            return role;
        }
    }
}

package fr.insee.vtl.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface Dataset extends Structured {

    List<List<Object>> getDataPoints();

    default List<Map<String, Object>> getDataAsMap() {
        return getDataPoints().stream().map(objects -> {
            return IntStream.range(0, getDataStructure().size()).boxed().collect(
                    Collectors.toMap(idx -> getDataStructure().get(idx).getName(), objects::get)
            );
        }).collect(Collectors.toList());
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

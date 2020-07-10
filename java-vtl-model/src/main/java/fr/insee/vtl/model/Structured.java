package fr.insee.vtl.model;

import java.util.Set;

public interface Structured {

    Set<String> getColumns();

    Class<?> getType(String col);

    Role getRole(String col);

    int getIndex(String col);

    enum Role {
        IDENTIFIER,
        MEASURE,
        ATTRIBUTE
    }

}

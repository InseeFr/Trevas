package fr.insee.vtl.model;

import java.util.Set;

public interface StructuredExpression {

    Set<String> getColumns();

    Class<?> getType(String col);

    int getIndex(String col);

}

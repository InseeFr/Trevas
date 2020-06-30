package fr.insee.vtl.model;

import java.util.Set;

public interface StructuredExpression {

    Set<String> getColumns();

    TypedExpression getColumn(String cul);

}

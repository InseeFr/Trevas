package fr.insee.vtl.model;

import java.util.List;
import java.util.Map;

/**
 * The <code>ListExpression</code> class is an abstract representation of an expression of type <code>List</code>.
 */
public abstract class ListExpression implements ResolvableExpression, TypedContainerExpression {

    @Override
    public Class<?> getType() {
        return List.class;
    }


    @Override
    public abstract List<?> resolve(Map<String, Object> context);
}

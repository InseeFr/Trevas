package fr.insee.vtl.model;

import java.util.Map;

/**
 * The <code>BooleanExpression</code> class is an abstract representation of a boolean expression.
 */
public abstract class BooleanExpression implements ResolvableExpression {

    @Override
    public abstract Boolean resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return Boolean.class;
    }
}

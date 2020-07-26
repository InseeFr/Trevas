package fr.insee.vtl.model;

import java.util.Map;

/**
 * The <code>StringExpression</code> class is an abstract representation of a string expression.
 */
public abstract class StringExpression implements ResolvableExpression {

    @Override
    public abstract String resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return String.class;
    }

}

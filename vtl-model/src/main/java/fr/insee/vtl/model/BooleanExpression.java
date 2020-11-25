package fr.insee.vtl.model;

import java.util.Map;

/**
 * The <code>BooleanExpression</code> class is an abstract representation of a boolean expression.
 */
public abstract class BooleanExpression implements ResolvableExpression {

    public static BooleanExpression of(Boolean value) {
        return new BooleanExpression() {
            @Override
            public Boolean resolve(Map<String, Object> context) {
                return value;
            }
        };
    }

    @Override
    public abstract Boolean resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return Boolean.class;
    }

    public static BooleanExpression of(VtlFunction<Map<String, Object>, Boolean> func) {
        return new BooleanExpression() {
            @Override
            public Boolean resolve(Map<String, Object> context) {
                return func.apply(context);
            }
        };
    }
}

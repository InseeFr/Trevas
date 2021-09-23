package fr.insee.vtl.model;

import java.util.Map;

/**
 * The <code>BooleanExpression</code> class is an abstract representation of a boolean expression.
 */
public abstract class BooleanExpression implements ResolvableExpression {

    /**
     * Builds a boolean expression that resolves to a given boolean value.
     *
     * @param value The boolean value the expression should resolve to.
     * @return A new boolean expression resolving to the given value.
     */
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

    /**
     * Builds a new boolean expression corresponding to a boolean function acting on input bindings.
     *
     * @param func A boolean function acting on input bindings.
     * @return A boolean expression corresponding to the given function.
     */
    public static BooleanExpression of(VtlFunction<Map<String, Object>, Boolean> func) {
        return new BooleanExpression() {
            @Override
            public Boolean resolve(Map<String, Object> context) {
                return func.apply(context);
            }
        };
    }
}

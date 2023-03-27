package fr.insee.vtl.model;

import java.util.Map;

/**
 * The <code>BooleanExpression</code> class is an abstract representation of a boolean expression.
 */
public abstract class BooleanExpression extends ResolvableExpression {
    public BooleanExpression(Positioned pos) {
        super(pos);
    }

    /**
     * Builds a boolean expression that resolves to a given boolean value.
     *
     * @param value The boolean value the expression should resolve to.
     * @return A new boolean expression resolving to the given value.
     */
    public static BooleanExpression of(Positioned pos, Boolean value) {
        return new BooleanExpression(pos) {
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
    public static BooleanExpression of(Positioned pos, VtlFunction<Map<String, Object>, Boolean> func) {
        return new BooleanExpression(pos) {
            @Override
            public Boolean resolve(Map<String, Object> context) {
                return func.apply(context);
            }
        };
    }

    /**
     * Returns the result of the cast operator on an expression
     *
     * @param expr        A <code>ResolvableExpression</code> to cast.
     * @param outputClass The type to cast expression.
     * @return The casted <code>ResolvableExpression</code>.
     */
    public static ResolvableExpression castTo(ResolvableExpression expr, Class<?> outputClass) {
        if (outputClass.equals(String.class))
            return StringExpression.of(context -> {
                Boolean exprValue = (Boolean) expr.resolve(context);
                if (exprValue == null) return null;
                return exprValue.toString();
            });
        if (outputClass.equals(Long.class))
            return LongExpression.of(context -> {
                Boolean exprValue = (Boolean) expr.resolve(context);
                if (exprValue == null) return null;
                return exprValue ? 1L : 0L;
            });
        if (outputClass.equals(Double.class))
            return DoubleExpression.of(context -> {
                Boolean exprValue = (Boolean) expr.resolve(context);
                if (exprValue == null) return null;
                return exprValue ? 1D : 0D;
            });
        if (outputClass.equals(Boolean.class))
            return BooleanExpression.of(expr, context -> {
                return (Boolean) expr.resolve(context);
            });
        throw new ClassCastException("Cast Boolean to " + outputClass + " is not supported");
    }
}

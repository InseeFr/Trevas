package fr.insee.vtl.model;

import java.util.Map;

/**
 * The <code>StringExpression</code> class is an abstract representation of a string expression.
 */
public abstract class StringExpression implements ResolvableExpression {

    public static StringExpression of(CharSequence value) {
        return new StringExpression() {
            @Override
            public String resolve(Map<String, Object> context) {
                return value == null ? null : value.toString();
            }
        };
    }

    @Override
    public abstract String resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return String.class;
    }

    public static StringExpression of(VtlFunction<Map<String, Object>, String> func) {
        return new StringExpression() {
            @Override
            public String resolve(Map<String, Object> context) {
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
    public static ResolvableExpression castTo(ResolvableExpression expr, Class outputClass) {
        if (outputClass.equals(String.class))
            return StringExpression.of(context -> {
                String exprValue = (String) expr.resolve(context);
                if (exprValue == null) return null;
                return exprValue;
            });
        if (outputClass.equals(Long.class))
            return LongExpression.of(context -> {
                String exprValue = (String) expr.resolve(context);
                if (exprValue == null) return null;
                return Long.valueOf(exprValue);
            });
        if (outputClass.equals(Double.class))
            return DoubleExpression.of(context -> {
                String exprValue = (String) expr.resolve(context);
                if (exprValue == null) return null;
                return Double.valueOf(exprValue);
            });
        if (outputClass.equals(Boolean.class))
            return BooleanExpression.of(context -> {
                String exprValue = (String) expr.resolve(context);
                if (exprValue == null) return null;
                return Boolean.valueOf(exprValue);
            });
        throw new ClassCastException("Cast String to " + outputClass + " is not supported");
    }

}

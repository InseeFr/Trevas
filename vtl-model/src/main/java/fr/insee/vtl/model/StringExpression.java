package fr.insee.vtl.model;

import java.util.Map;

/**
 * The <code>StringExpression</code> class is an abstract representation of a string expression.
 */
public abstract class StringExpression implements ResolvableExpression {

    /**
     * Constructs a string expression evaluating to a given sequence of characters.
     *
     * @param value The character sequence that the expression evaluates to.
     * @return The resulting string expression.
     */
    public static StringExpression of(CharSequence value) {
        return new StringExpression() {
            @Override
            public String resolve(Map<String, Object> context) {
                return value.toString();
            }
        };
    }

    @Override
    public abstract String resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return String.class;
    }

    /**
     * Construct a string expression evaluating to the result of a string function applied to the context.
     *
     * @param func The string function to apply to the context.
     * @return The resulting string expression.
     */
    public static StringExpression of(VtlFunction<Map<String, Object>, String> func) {
        return new StringExpression() {
            @Override
            public String resolve(Map<String, Object> context) {
                return func.apply(context);
            }
        };
    }

}

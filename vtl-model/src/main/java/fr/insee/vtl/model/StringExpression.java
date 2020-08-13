package fr.insee.vtl.model;

import java.util.Map;
import java.util.function.Function;

/**
 * The <code>StringExpression</code> class is an abstract representation of a string expression.
 */
public abstract class StringExpression implements ResolvableExpression {

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

    public static StringExpression of(Function<Map<String, Object>, String> func) {
        return new StringExpression() {
            @Override
            public String resolve(Map<String, Object> context) {
                return func.apply(context);
            }
        };
    }

}

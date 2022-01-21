package fr.insee.vtl.model;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
                return value == null ? null : value.toString();
            }
        };
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

    /**
     * Returns the result of the cast operator on an expression
     *
     * @param expr        A <code>ResolvableExpression</code> to cast.
     * @param outputClass The type to cast expression.
     * @return The casted <code>ResolvableExpression</code>.
     */
    public static ResolvableExpression castTo(ResolvableExpression expr, Class<?> outputClass, String mask) {
        if (outputClass.equals(String.class))
            return StringExpression.of(context -> (String) expr.resolve(context));
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
        if (outputClass.equals(Instant.class))
            return InstantExpression.of(context -> {
                if (mask == null) return null;
                String exprValue = (String) expr.resolve(context);
                if (exprValue == null) return null;
                DateTimeFormatter maskFormatter = DateTimeFormatter.ofPattern(mask).withZone(ZoneOffset.UTC);
                return LocalDateTime.parse(exprValue, maskFormatter).toInstant(ZoneOffset.UTC);
            });
        throw new ClassCastException("Cast String to " + outputClass + " is not supported");
    }

    @Override
    public abstract String resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return String.class;
    }

}

package fr.insee.vtl.model;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * The <code>StringExpression</code> class is an abstract representation of a date expression.
 */
public abstract class InstantExpression implements ResolvableExpression {

    /**
     * Constructs a date expression evaluating to a given date.
     *
     * @param value The date value the expression should resolve to.
     * @return The resulting date expression.
     */
    public static InstantExpression of(Instant value) {
        return new InstantExpression() {
            @Override
            public Instant resolve(Map<String, Object> context) {
                return value;
            }
        };
    }

    /**
     * Construct a date expression evaluating to the result of a date function applied to the context.
     *
     * @param func The date function to apply to the context.
     * @return The resulting date expression.
     */
    public static InstantExpression of(VtlFunction<Map<String, Object>, Instant> func) {
        return new InstantExpression() {
            @Override
            public Instant resolve(Map<String, Object> context) {
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
            return StringExpression.of(context -> {
                if (mask == null) {
                    throw new IllegalArgumentException("missing mask");
                }
                Instant exprValue = (Instant) expr.resolve(context);
                if (exprValue == null) return null;
                DateTimeFormatter maskFormatter = DateTimeFormatter.ofPattern(mask);
                return maskFormatter.format(exprValue.atOffset(ZoneOffset.UTC));
            });
        throw new ClassCastException("Cast Date to " + outputClass + " is not supported");
    }

    @Override
    public abstract Instant resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return Instant.class;
    }

}

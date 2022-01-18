package fr.insee.vtl.model;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * The <code>StringExpression</code> class is an abstract representation of a date expression.
 */
public abstract class DateExpression implements ResolvableExpression {

    /**
     * Constructs a date expression evaluating to a given date.
     *
     * @param value The date value the expression should resolve to.
     * @return The resulting date expression.
     */
    public static DateExpression of(LocalDate value) {
        return new DateExpression() {
            @Override
            public LocalDate resolve(Map<String, Object> context) {
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
    public static DateExpression of(VtlFunction<Map<String, Object>, LocalDate> func) {
        return new DateExpression() {
            @Override
            public LocalDate resolve(Map<String, Object> context) {
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
                if (mask == null) return null;
                LocalDate exprValue = (LocalDate) expr.resolve(context);
                if (exprValue == null) return null;
                DateTimeFormatter maskFormatter = DateTimeFormatter.ofPattern(mask);
                return maskFormatter.format(exprValue);
            });
        throw new ClassCastException("Cast Date to " + outputClass + " is not supported");
    }

    @Override
    public abstract LocalDate resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return LocalDate.class;
    }

}

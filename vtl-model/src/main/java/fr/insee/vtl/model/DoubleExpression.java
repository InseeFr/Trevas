package fr.insee.vtl.model;

import java.util.Map;

/**
 * The <code>DoubleExpression</code> class is an abstract representation of an expression of type <code>Double</code>.
 */
public abstract class DoubleExpression extends NumberExpression {

    /**
     * Returns the result of applying a function of type <code>Double</code> to a given dataset context.
     *
     * @param func A function applicable to a dataset context and yielding a <code>Double</code> result.
     * @return The result of applying the given function to the dataset context.
     */
    public static DoubleExpression of(VtlFunction<Map<String, Object>, Double> func) {
        return new DoubleExpression() {
            @Override
            public Double resolve(Map<String, Object> context) {
                return func.apply(context);
            }
        };
    }

    /**
     * Returns a double expression evaluating to a given value in any context.
     *
     * @param value The double value to which the expression should evaluate.
     * @return The double expression.
     */
    public static DoubleExpression of(Double value) {
        return new DoubleExpression() {
            @Override
            public Double resolve(Map<String, Object> context) {
                return value;
            }
        };
    }

    @Override
    public abstract Double resolve(Map<String, Object> context);

    @Override
    public Class<Double> getType() {
        return Double.class;
    }
}

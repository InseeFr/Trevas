package fr.insee.vtl.model;

/**
 * The <code>NumberExpression</code> class is an abstract representation of a numeric expression.
 */
public abstract class NumberExpression extends ResolvableExpression {

    public NumberExpression() {
        super(() -> {throw new UnsupportedOperationException("TODO");});
    }

    @Override
    public Class<? extends Number> getType() {
        return Number.class;
    }
}
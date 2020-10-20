package fr.insee.vtl.model;

/**
 * The <code>NumberExpression</code> class is an abstract representation of a numeric expression.
 */
public abstract class NumberExpression implements ResolvableExpression {

    @Override
    public Class<? extends Number> getType() {
        return Number.class;
    }
}
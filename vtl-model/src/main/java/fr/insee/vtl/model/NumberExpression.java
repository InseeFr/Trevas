package fr.insee.vtl.model;

abstract class NumberExpression implements ResolvableExpression {

    @Override
    public Class<? extends Number> getType() {
        return Number.class;
    }
}
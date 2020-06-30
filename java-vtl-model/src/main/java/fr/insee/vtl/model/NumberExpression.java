package fr.insee.vtl.model;

import javax.script.ScriptContext;

public class NumberExpression implements ResolvableExpression {
    @Override
    public Number resolve(ScriptContext context) {
        return null;
    }

    @Override
    public Class<? extends Number> getType() {
        return Number.class;
    }
}
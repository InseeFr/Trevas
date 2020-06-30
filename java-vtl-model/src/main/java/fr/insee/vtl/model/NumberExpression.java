package fr.insee.vtl.model;

import javax.script.ScriptContext;
import java.util.Map;

public class NumberExpression implements ResolvableExpression {
    @Override
    public Number resolve(ScriptContext context) {
        return null;
    }

    @Override
    public Object resolve(Map<String, Object> context) {
        throw new UnsupportedOperationException("TODO: refactor");
    }

    @Override
    public Class<? extends Number> getType() {
        return Number.class;
    }
}
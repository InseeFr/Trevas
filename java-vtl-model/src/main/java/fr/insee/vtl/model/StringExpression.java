package fr.insee.vtl.model;

import java.util.Map;

public abstract class StringExpression implements ResolvableExpression {

    @Override
    public abstract String resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return String.class;
    }

}

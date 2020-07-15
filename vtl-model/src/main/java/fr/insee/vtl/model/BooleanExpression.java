package fr.insee.vtl.model;

import java.util.Map;

public abstract class BooleanExpression implements ResolvableExpression {

    @Override
    public abstract Boolean resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return Boolean.class;
    }
}

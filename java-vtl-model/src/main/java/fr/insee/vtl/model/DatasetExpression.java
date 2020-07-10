package fr.insee.vtl.model;

import java.util.Map;

public abstract class DatasetExpression implements ResolvableExpression, Structured {

    @Override
    public abstract Dataset resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return Dataset.class;
    }
}

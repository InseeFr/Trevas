package fr.insee.vtl.model;

import java.util.List;
import java.util.Map;

public abstract class ListExpression implements ResolvableExpression, TypedContainerExpression {

    @Override
    public Class<?> getType() {
        return List.class;
    }


    @Override
    public abstract List<?> resolve(Map<String, Object> context);
}

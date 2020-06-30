package fr.insee.vtl.model;

import javax.script.ScriptContext;
import java.util.Map;

public abstract class DatasetExpression implements ResolvableExpression, StructuredExpression {

    @Override
    public abstract DatasetWrapper resolve(ScriptContext context);

    @Override
    public abstract DatasetWrapper resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return DatasetWrapper.class;
    }
}

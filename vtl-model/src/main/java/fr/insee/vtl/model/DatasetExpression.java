package fr.insee.vtl.model;

import java.util.List;
import java.util.Map;

/**
 * The <code>DatasetExpression</code> class is an abstract representation of a dataset expression.
 */
public abstract class DatasetExpression implements ResolvableExpression, Structured {

    public static DatasetExpression of(Dataset value) {
        return new DatasetExpression() {

            @Override
            public List<Dataset.Component> getDataStructure() {
                return value.getDataStructure();
            }

            @Override
            public Dataset resolve(Map<String, Object> na) {
                return value;
            }
        };
    }

    @Override
    public abstract Dataset resolve(Map<String, Object> context);

    @Override
    public Class<?> getType() {
        return Dataset.class;
    }
}

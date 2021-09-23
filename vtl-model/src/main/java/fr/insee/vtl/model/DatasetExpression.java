package fr.insee.vtl.model;

import java.util.Map;

/**
 * The <code>DatasetExpression</code> class is an abstract representation of a dataset expression.
 */
public abstract class DatasetExpression implements ResolvableExpression, Structured {

    /**
     * Returns a dataset expression based on a given dataset.
     *
     * @param value The dataset on which the expression should be based.
     * @return The dataset expression.
     */
    public static DatasetExpression of(Dataset value) {
        return new DatasetExpression() {

            @Override
            public Structured.DataStructure getDataStructure() {
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

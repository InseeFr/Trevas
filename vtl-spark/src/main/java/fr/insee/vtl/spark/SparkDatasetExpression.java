package fr.insee.vtl.spark;

import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Structured;

import java.util.Map;
import java.util.Objects;

public class SparkDatasetExpression extends DatasetExpression {

    private final SparkDataset dataset;

    public SparkDatasetExpression(SparkDataset dataset) {
        this.dataset = Objects.requireNonNull(dataset);
    }

    @Override
    public SparkDataset resolve(Map<String, Object> context) {
        return dataset;
    }

    @Override
    public Structured.DataStructure  getDataStructure() {
        return dataset.getDataStructure();
    }
}

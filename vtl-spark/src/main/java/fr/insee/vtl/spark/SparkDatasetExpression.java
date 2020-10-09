package fr.insee.vtl.spark;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;

import java.util.List;
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
    public List<Dataset.Component> getDataStructure() {
        return dataset.getDataStructure();
    }
}

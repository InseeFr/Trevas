package fr.insee.vtl.spark;

import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SparkProcessingEngine implements ProcessingEngine {

    private final SparkSession spark;

    public SparkProcessingEngine(SparkSession spark) {
        this.spark = Objects.requireNonNull(spark);
    }

    public SparkProcessingEngine() {
        this.spark = SparkSession.active();
    }

    @Override
    public DatasetExpression executeCalc(DatasetExpression expression, Map<String, ResolvableExpression> expressions) {
        return null;
    }

    @Override
    public DatasetExpression executeFilter(DatasetExpression expression, ResolvableExpression filter) {
        return null;
    }

    @Override
    public DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo) {
        return null;
    }

    @Override
    public DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames) {
        SparkDataset dataset;
        if (expression instanceof SparkDatasetExpression) {
            dataset = ((SparkDatasetExpression) expression).resolve(Map.of());
        } else {
            dataset = new SparkDataset(expression.resolve(Map.of()), spark);
        }

        List<Column> columns = columnNames.stream().map(Column::new).collect(Collectors.toList());
        Seq<Column> columnSeq = JavaConverters.iterableAsScalaIterable(columns).toSeq();

        // Project in spark.
        Dataset<Row> result = dataset.getSparkDataset().select(columnSeq);

        return new SparkDatasetExpression(new SparkDataset(result));
    }
}

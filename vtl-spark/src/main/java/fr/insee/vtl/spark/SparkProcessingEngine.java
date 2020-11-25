package fr.insee.vtl.spark;

import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ProcessingEngineFactory;
import fr.insee.vtl.model.ResolvableExpression;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import javax.script.ScriptEngine;
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
    public DatasetExpression executeCalc(DatasetExpression expression, Map<String, ResolvableExpression> expressions, Map<String, fr.insee.vtl.model.Dataset.Role> roles) {
        return null;
    }

    @Override
    public DatasetExpression executeFilter(DatasetExpression expression, ResolvableExpression filter) {
        SparkDataset dataset;
        if (expression instanceof SparkDatasetExpression) {
            dataset = ((SparkDatasetExpression) expression).resolve(Map.of());
        } else {
            dataset = new SparkDataset(expression.resolve(Map.of()), spark);
        }

        Dataset<Row> ds = dataset.getSparkDataset();
        SparkFilterFunction filterFunction = new SparkFilterFunction(filter);
        Dataset<Row> result = ds.filter(filterFunction);
        return new SparkDatasetExpression(new SparkDataset(result));
    }

    @Override
    public DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo) {
        SparkDataset dataset;
        if (expression instanceof SparkDatasetExpression) {
            dataset = ((SparkDatasetExpression) expression).resolve(Map.of());
        } else {
            dataset = new SparkDataset(expression.resolve(Map.of()), spark);
        }

        List<Column> newNames = fromTo.entrySet().stream()
                .map(rename -> new Column(rename.getKey()).as(rename.getValue()))
                .collect(Collectors.toList());

        Dataset<Row> result = dataset.getSparkDataset().select(JavaConverters.iterableAsScalaIterable(newNames).toSeq());

        return new SparkDatasetExpression(new SparkDataset(result));
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

    public static class Factory implements ProcessingEngineFactory {

        private static final String SPARK_SESSION = "$vtl.spark.session";

        @Override
        public String getName() {
            return "spark";
        }

        @Override
        public ProcessingEngine getProcessingEngine(ScriptEngine engine) {
            // Try to find the session in the script engine.
            var session = engine.get(SPARK_SESSION);
            if (session != null) {
                if (session instanceof SparkSession) {
                    return new SparkProcessingEngine((SparkSession) session);
                } else {
                    throw new IllegalArgumentException(SPARK_SESSION + " was not a spark session");
                }
            } else {
                var activeSession = SparkSession.active();
                if (activeSession != null) {
                    return new SparkProcessingEngine(activeSession);
                } else {
                    throw new IllegalArgumentException("no active spark session");
                }
            }
        }
    }

}

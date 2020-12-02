package fr.insee.vtl.spark;

import fr.insee.vtl.model.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import javax.script.ScriptEngine;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static fr.insee.vtl.spark.SparkDataset.fromVtlType;

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
        SparkDataset dataset;
        if (expression instanceof SparkDatasetExpression) {
            dataset = ((SparkDatasetExpression) expression).resolve(Map.of());
        } else {
            dataset = new SparkDataset(expression.resolve(Map.of()), spark);
        }
        Dataset<Row> ds = dataset.getSparkDataset();

        // Compute the new schema.
        // TODO: Use conversion from DataStructure.
        StructType oldSchema = ds.schema();
        List<String> oldNames = Arrays.asList(oldSchema.fieldNames());
        List<String> newNames = new ArrayList<>(oldNames);
        for (String exprName : expressions.keySet()) {
            if (!newNames.contains(exprName)) {
                newNames.add(exprName);
            }
        }

        List<StructField> newFields = new ArrayList<>();
        for (String newName : newNames) {
            if (oldNames.contains(newName) && !expressions.containsKey(newName)) {
                newFields.add(oldSchema.apply(newName));
            } else {
                newFields.add(DataTypes.createStructField(
                        newName,
                        fromVtlType(expressions.get(newName).getType()),
                        false
                ));
            }
        }
        StructType newSchema = DataTypes.createStructType(newFields);

        Dataset<Row> result = ds.map((MapFunction<Row, Row>) row -> {
            SparkRowMap context = new SparkRowMap(row);
            Object[] objects = new Object[newSchema.size()];
            for (String name : newSchema.fieldNames()) {
                int index = newSchema.fieldIndex(name);
                if (expressions.containsKey(name)) {
                    objects[index] = expressions.get(name).resolve(context);
                } else {
                    objects[index] = row.get(index);
                }
            }
            return new GenericRowWithSchema(objects, newSchema);
        }, RowEncoder.apply(newSchema));

        return new SparkDatasetExpression(new SparkDataset(result));
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

    @Override
    public DatasetExpression executeUnion(List<DatasetExpression> datasets) {
        return null;
    }

    @Override
    public DatasetExpression executeAggr(DatasetExpression expression, Structured.DataStructure structure,
                                         Map<String, AggregationExpression> collectorMap,
                                         Function<Structured.DataPoint, Map<String, Object>> keyExtractor) {
        return null;
    }
}

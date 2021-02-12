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
import scala.collection.Seq;

import javax.script.ScriptEngine;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static fr.insee.vtl.model.Dataset.Component;
import static fr.insee.vtl.model.Dataset.Role;
import static fr.insee.vtl.model.Dataset.Role.IDENTIFIER;
import static fr.insee.vtl.spark.SparkDataset.fromVtlType;
import static scala.collection.JavaConverters.iterableAsScalaIterable;

public class SparkProcessingEngine implements ProcessingEngine {

    private final SparkSession spark;

    public SparkProcessingEngine(SparkSession spark) {
        this.spark = Objects.requireNonNull(spark);
    }

    public SparkProcessingEngine() {
        this.spark = SparkSession.active();
    }

    private static Map<String, Role> getRoleMap(Collection<Component> components) {
        return components.stream()
                .collect(Collectors.toMap(
                        Component::getName,
                        Component::getRole
                ));
    }

    private static Map<String, Role> getRoleMap(fr.insee.vtl.model.Dataset dataset) {
        return getRoleMap(dataset.getDataStructure().values());
    }

    private SparkDataset asSparkDataset(DatasetExpression expression) {
        if (expression instanceof SparkDatasetExpression) {
            return ((SparkDatasetExpression) expression).resolve(Map.of());
        } else {
            var dataset = expression.resolve(Map.of());
            return new SparkDataset(dataset, getRoleMap(dataset), spark);
        }
    }

    @Override
    public DatasetExpression executeCalc(DatasetExpression expression, Map<String, ResolvableExpression> expressions, Map<String, Role> roles) {
        SparkDataset dataset = asSparkDataset(expression);
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

        // Create the new role map.
        var roleMap = getRoleMap(dataset);
        roleMap.putAll(roles);

        return new SparkDatasetExpression(new SparkDataset(result, roleMap));
    }

    @Override
    public DatasetExpression executeFilter(DatasetExpression expression, ResolvableExpression filter) {
        SparkDataset dataset = asSparkDataset(expression);

        Dataset<Row> ds = dataset.getSparkDataset();
        SparkFilterFunction filterFunction = new SparkFilterFunction(filter);
        Dataset<Row> result = ds.filter(filterFunction);
        return new SparkDatasetExpression(new SparkDataset(result, getRoleMap(dataset)));
    }

    @Override
    public DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo) {
        SparkDataset dataset = asSparkDataset(expression);

        List<Column> columns = new ArrayList<>();
        for (String name : dataset.getColumnNames()) {
            var column = new Column(name);
            if (fromTo.containsKey(name)) {
                column = column.as(fromTo.get(name));
            }
            columns.add(column);
        }

        Dataset<Row> result = dataset.getSparkDataset().select(iterableAsScalaIterable(columns).toSeq());

        var roleMap = getRoleMap(dataset);
        for (Map.Entry<String, String> fromToEntry : fromTo.entrySet()) {
            var role = roleMap.remove(fromToEntry.getKey());
            roleMap.put(fromToEntry.getValue(), role);
        }

        return new SparkDatasetExpression(new SparkDataset(result, roleMap));
    }

    @Override
    public DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames) {
        SparkDataset dataset = asSparkDataset(expression);

        List<Column> columns = columnNames.stream().map(Column::new).collect(Collectors.toList());
        Seq<Column> columnSeq = iterableAsScalaIterable(columns).toSeq();

        // Project in spark.
        Dataset<Row> result = dataset.getSparkDataset().select(columnSeq);

        return new SparkDatasetExpression(new SparkDataset(result, getRoleMap(dataset)));
    }

    @Override
    public DatasetExpression executeUnion(List<DatasetExpression> datasets) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public DatasetExpression executeAggr(DatasetExpression expression, Structured.DataStructure structure,
                                         Map<String, AggregationExpression> collectorMap,
                                         Function<Structured.DataPoint, Map<String, Object>> keyExtractor) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public DatasetExpression executeInnerJoin(Map<String, DatasetExpression> datasets, List<Component> components) {
        List<Dataset<Row>> sparkDatasets = toAliasedDatasets(datasets);
        List<String> identifiers = identifierNames(components);
        var innerJoin = executeJoin(sparkDatasets, identifiers, "inner");
        return new SparkDatasetExpression(new SparkDataset(innerJoin, getRoleMap(components)));
    }

    @Override
    public DatasetExpression executeLeftJoin(Map<String, DatasetExpression> datasets, List<Structured.Component> components) {
        List<Dataset<Row>> sparkDatasets = toAliasedDatasets(datasets);
        List<String> identifiers = identifierNames(components);
        var innerJoin = executeJoin(sparkDatasets, identifiers, "left");
        return new SparkDatasetExpression(new SparkDataset(innerJoin, getRoleMap(components)));
    }

    private List<Dataset<Row>> toAliasedDatasets(Map<String, DatasetExpression> datasets) {
        List<Dataset<Row>> sparkDatasets = new ArrayList<>();
        for (Map.Entry<String, DatasetExpression> dataset : datasets.entrySet()) {
            var sparkDataset = asSparkDataset(dataset.getValue())
                    .getSparkDataset()
                    .as(dataset.getKey());
            sparkDatasets.add(sparkDataset);
        }
        return sparkDatasets;
    }

    private static List<String> identifierNames(List<Component> components) {
        return components.stream()
                .filter(component -> IDENTIFIER.equals(component.getRole()))
                .map(Component::getName)
                .collect(Collectors.toList());
    }

    public Dataset<Row> executeJoin(List<Dataset<Row>> sparkDatasets, List<String> identifiers, String type) {
        var iterator = sparkDatasets.iterator();
        var result = iterator.next();
        while (iterator.hasNext()) {
            result = result.join(
                    iterator.next(),
                    iterableAsScalaIterable(identifiers).toSeq(),
                    type
            );
        }
        return result;
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

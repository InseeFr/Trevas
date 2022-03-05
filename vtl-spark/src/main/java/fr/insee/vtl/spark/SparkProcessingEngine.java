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
import scala.collection.JavaConverters;
import scala.collection.mutable.ListBuffer;

import javax.script.ScriptEngine;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static fr.insee.vtl.model.Dataset.Component;
import static fr.insee.vtl.model.Dataset.Role;
import static fr.insee.vtl.model.Dataset.Role.IDENTIFIER;
import static fr.insee.vtl.spark.SparkDataset.fromVtlType;
import static org.apache.spark.sql.functions.*;
import static scala.collection.JavaConverters.iterableAsScalaIterable;

/**
 * The <code>SparkProcessingEngine</code> class is an implementation of a VTL engine using Apache Spark.
 */
public class SparkProcessingEngine implements ProcessingEngine {

    private final SparkSession spark;

    /**
     * Constructor taking an existing Spark session.
     *
     * @param spark The Spark session to use for the engine.
     */
    public SparkProcessingEngine(SparkSession spark) {
        this.spark = Objects.requireNonNull(spark);
    }

    /**
     * Constructor creating an engine with the currently active Spark session (or otherwise the default one).
     */
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
            if (dataset instanceof SparkDataset) {
                return (SparkDataset) dataset;
            } else {
                return new SparkDataset(dataset, getRoleMap(dataset), spark);
            }
        }
    }

    @Override
    public DatasetExpression executeCalc(DatasetExpression expression, Map<String, ResolvableExpression> expressions,
                                         Map<String, Role> roles, Map<String, String> expressionStrings) {
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
                        true
                ));
            }
        }
        StructType newSchema = DataTypes.createStructType(newFields);

        // Create the new role map.
        var roleMap = getRoleMap(dataset);
        roleMap.putAll(roles);

        try {
            // Try with expressions first.
            var expressionsList = expressionStrings.entrySet().stream().map(expressionEntry -> String.format(
                    "%s as %s", expressionEntry.getValue(), expressionEntry.getKey()
            )).collect(Collectors.toList());
            Dataset<Row> result = ds.selectExpr(Stream.concat(
                    // Get back the old columns.
                    newNames.stream().filter(name -> !expressionStrings.containsKey(name)),
                    expressionsList.stream()
            ).toArray(String[]::new));
            return new SparkDatasetExpression(new SparkDataset(result, roleMap));
        } catch (Exception e) {
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
            return new SparkDatasetExpression(new SparkDataset(result, roleMap));
        }
    }

    @Override
    public DatasetExpression executeFilter(DatasetExpression expression, BooleanExpression filter, String filterText) {
        SparkDataset dataset = asSparkDataset(expression);

        Dataset<Row> ds = dataset.getSparkDataset();
        try {
            Dataset<Row> result = ds.filter(filterText);
            return new SparkDatasetExpression(new SparkDataset(result, getRoleMap(dataset)));
        } catch (Exception e) {
            SparkFilterFunction filterFunction = new SparkFilterFunction(filter);
            Dataset<Row> result = ds.filter(filterFunction);
            return new SparkDatasetExpression(new SparkDataset(result, getRoleMap(dataset)));
        }
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
    public DatasetExpression executeAggr(DatasetExpression dataset, Structured.DataStructure structure,
                                         Map<String, AggregationExpression> collectorMap,
                                         Function<Structured.DataPoint, Map<String, Object>> keyExtractor) {

        /*
        * for the aggregation function map, we may have duplicate keys, if we use column as key.
        * for example, column age, can have min, max, mean, etc. as function.
        * 
        * For the aliasCol map, it is the same thing
        *
        * Solution, we need to use Data Structure LinkedHashMap<String,List<String>> to store multiple actions on the
        * same column, and preserve the order of insertion
         */

        Map<String,List<String>> operations=new LinkedHashMap<>();
        List ageColFunctions=new ArrayList();
        List weightFunctions=new ArrayList();
        List nullFunctions=new ArrayList();
        
        operations.put("age",ageColFunctions);
        operations.get("age").add("sum");
        operations.get("age").add("max");
        operations.get("age").add("min");
        //operations.get("age").add("median");
        operations.put("weight",weightFunctions);
        operations.get("weight").add("max");
        //operations.get("weight").add("median");
        operations.put("null",nullFunctions);
        operations.get("null").add("count");

        Map<String,List<String>> aliases=new HashMap<>();
        List ageAliasCols=new ArrayList();
        List weightAliasCols=new ArrayList();
        List nullAliasCols=new ArrayList();

        aliases.put("age",ageAliasCols);
        aliases.get("age").add("sumAge");
        aliases.get("age").add("maxAge");
        aliases.get("age").add("minAge");
        //aliases.get("age").add("medianAge");
        aliases.put("weight",weightAliasCols);
        aliases.get("weight").add("maxWeight");
        //operations.get("weight").add("medianWeight");
        aliases.put("null",nullAliasCols);
        aliases.get("null").add("countVal");


        return executeAggr(
                asSparkDataset(dataset),
                List.of("country", "age"),
                operations,
                aliases);
    }

    public DatasetExpression executeAggr(SparkDataset dataset, List<String> groupByColumns,
                                         Map<String,List<String>> operations, Map<String,List<String>> aliases) {
        /*
        * - agg(java.util.Map<String,String> exprs)
        * - agg(Column expr, scala.collection.Seq<Column> exprs)
        * */
        Dataset<Row> df=dataset.getSparkDataset();

//        Column head = sum("age").alias("sumAge");
//        List<Column> columns=new ArrayList<>();
//        columns.add(avg("weight").alias("avgWeight"));
//        columns.add(max("age").alias("maxAge"));
//        columns.add(min("age").alias("minAge"));
//        columns.add(count("*").alias("countVal"));

        SparkAggrFuncExprBuilder builder = null;
        try{
            builder=new SparkAggrFuncExprBuilder(operations,aliases);
        }
        catch (Exception e){

        }
        List<Column> columns=builder.getTailExpressions();
        Column head=builder.getHeadExpression();
        Seq<Column> tails=iterableAsScalaIterable(columns).toSeq();
        // in scala groupByColumns.head, groupByColumns.tail:_*
        Dataset<Row> result = df.groupBy(groupByColumns.get(0))
                .agg(head,tails);


        result.show();
        return new SparkDatasetExpression(new SparkDataset(result));
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

    @Override
    public DatasetExpression executeCrossJoin(Map<String, DatasetExpression> datasets, List<Component> identifiers) {
        List<Dataset<Row>> sparkDatasets = toAliasedDatasets(datasets);
        var crossJoin = executeJoin(sparkDatasets, List.of(), "cross");
        return new SparkDatasetExpression(new SparkDataset(crossJoin, getRoleMap(identifiers)));
    }

    @Override
    public DatasetExpression executeFullJoin(Map<String, DatasetExpression> datasets, List<Component> identifiers) {
        List<Dataset<Row>> sparkDatasets = toAliasedDatasets(datasets);
        List<String> identifierNames = identifierNames(identifiers);
        var crossJoin = executeJoin(sparkDatasets, identifierNames, "outer");
        return new SparkDatasetExpression(new SparkDataset(crossJoin, getRoleMap(identifiers)));
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

    /**
     * Utility method used for the implementation of the different types of join operations.
     *
     * @param sparkDatasets a list datasets.
     * @param identifiers the list of identifiers to join on.
     * @param type the type of join operation.
     * @return The dataset resulting from the join operation.
     */
    public Dataset<Row> executeJoin(List<Dataset<Row>> sparkDatasets, List<String> identifiers, String type) {
        var iterator = sparkDatasets.iterator();
        var result = iterator.next();
        while (iterator.hasNext()) {
            if (type.equals("cross")) result = result.crossJoin(iterator.next());
            else result = result.join(
                    iterator.next(),
                    iterableAsScalaIterable(identifiers).toSeq(),
                    type
            );
        }
        return result;
    }

    /**
     * The <code>Factory</code> class is an implementation of a VTL engine factory that returns Spark engines.
     */
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

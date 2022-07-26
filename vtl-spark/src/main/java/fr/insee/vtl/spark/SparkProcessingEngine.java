package fr.insee.vtl.spark;

import fr.insee.vtl.model.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import javax.script.ScriptEngine;
import java.util.*;
import java.util.stream.Collectors;

import static fr.insee.vtl.model.AggregationExpression.*;
import static fr.insee.vtl.model.Dataset.Component;
import static fr.insee.vtl.model.Dataset.Role;
import static fr.insee.vtl.model.Dataset.Role.IDENTIFIER;
import static fr.insee.vtl.spark.SparkDataset.fromVtlType;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.*;
import static scala.collection.JavaConverters.iterableAsScalaIterable;

/**
 * The <code>SparkProcessingEngine</code> class is an implementation of a VTL engine using Apache Spark.
 */
public class SparkProcessingEngine implements ProcessingEngine {

    public static final Integer DEFAULT_MEDIAN_ACCURACY = 1000000;
    private final SparkSession spark;


    /**
     * Constructor taking an existing Spark session.
     *
     * @param spark The Spark session to use for the engine.
     */
    public SparkProcessingEngine(SparkSession spark) {
        this.spark = Objects.requireNonNull(spark);
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

    // TODO: (expression instanceof MinAggregationExpression)
    // TODO column = stddev_pop(columnName);
    private static Column convertAggregation(String columnName, AggregationExpression expression) throws UnsupportedOperationException {
        Column column;
        if (expression instanceof MinAggregationExpression) {
            column = min(columnName);
        } else if (expression instanceof MaxAggregationExpression) {
            column = max(columnName);
        } else if (expression instanceof AverageAggregationExpression) {
            column = avg(columnName);
        } else if (expression instanceof SumAggregationExpression) {
            column = sum(columnName);
        } else if (expression instanceof CountAggregationExpression) {
            column = count("*");
        } else if (expression instanceof MedianAggregationExpression) {
            column = percentile_approx(col(columnName), lit(0.5), lit(DEFAULT_MEDIAN_ACCURACY));
        } else if (expression instanceof StdDevSampAggregationExpression) {
            column = stddev_samp(columnName);
        } else if (expression instanceof VarPopAggregationExpression) {
            column = var_pop(columnName);
        } else if (expression instanceof VarSampAggregationExpression) {
            column = var_samp(columnName);
        } else {
            throw new UnsupportedOperationException("unknown aggregation " + expression.getClass());
        }
        return column.alias(columnName);
    }

    private static WindowSpec buildWindowSpec(List<String> partitionBy) {
        return buildWindowSpec(partitionBy, null, null);
    }

    private static WindowSpec buildWindowSpec(List<String> partitionBy,
                                              Map<String, Analytics.Order> orderBy) {
        return buildWindowSpec(partitionBy, orderBy, null);
    }

    private static WindowSpec buildWindowSpec(List<String> partitionBy,
                                              Map<String, Analytics.Order> orderBy,
                                              Analytics.WindowSpec window) {
        WindowSpec windowSpec = null;
        // 1.1 apply partition spec
        if (partitionBy != null && partitionBy.size() > 0) {
            windowSpec = Window.partitionBy(colNameToCol(partitionBy));
        }
        // 1.2 apply orderBy spec

        // check if it has orderBy clause
        if (orderBy != null && orderBy.size() > 0) {
            Seq<Column> orders = buildOrderCol(orderBy);
            // if it has partitionBy clause, add orderBy after partitionBy
            if (windowSpec != null) windowSpec = windowSpec.orderBy(orders);
                // if not, create a window spec to put orderBy clause
            else windowSpec = Window.orderBy(orders);
        }
        // 1.3 apply window frame spec
        if (window != null) {
            // check if windowSpec has partitionBy, and/or orderBy before
            if (windowSpec != null) {
                // if it's a data point
                if (window instanceof Analytics.DataPointWindow) {
                    windowSpec = windowSpec.rowsBetween(-window.getLower(), window.getUpper());
                } else if (window instanceof Analytics.RangeWindow) {
                    windowSpec = windowSpec.rangeBetween(-window.getLower(), window.getUpper());
                }
            } else {
                if (window instanceof Analytics.DataPointWindow) {
                    windowSpec = Window.rowsBetween(-window.getLower(), window.getUpper());
                } else if (window instanceof Analytics.RangeWindow) {
                    windowSpec = Window.rangeBetween(-window.getLower(), window.getUpper());
                }
            }
        }
        return windowSpec;
    }

    public static Seq<Column> colNameToCol(List<String> inputColNames) {
        List<Column> cols = new ArrayList<>();
        for (String colName : inputColNames) {
            cols.add(col(colName));
        }
        return JavaConverters.asScalaIteratorConverter(cols.iterator()).asScala().toSeq();
    }

    // helper function that builds order col expression with asc and desc spec
    public static Seq<Column> buildOrderCol(Map<String, Analytics.Order> orderCols) {
        List<Column> orders = new ArrayList<>();
        for (Map.Entry<String, Analytics.Order> entry : orderCols.entrySet()) {
            if (entry.getValue().equals(Analytics.Order.DESC)) {
                orders.add(col(entry.getKey()).desc());
            } else {
                orders.add(col(entry.getKey()));
            }
        }
        return JavaConverters.asScalaIteratorConverter(orders.iterator()).asScala().toSeq();
    }

    private static List<String> identifierNames(List<Component> components) {
        return components.stream()
                .filter(component -> IDENTIFIER.equals(component.getRole()))
                .map(Component::getName)
                .collect(Collectors.toList());
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

        // Rename all the columns to avoid conflicts (static single assignment).
        Map<String, String> aliasesToName = new HashMap<>();
        Map<String, ResolvableExpression> renamedExpressions = new LinkedHashMap<>();
        Map<String, String> renamedExpressionString = new LinkedHashMap<>();
        for (var name : expressions.keySet()) {
            String alias = name + "_" + aliasesToName.size();
            renamedExpressions.put(alias, expressions.get(name));
            renamedExpressionString.put(alias, expressionStrings.get(name));
            aliasesToName.put(alias, name);
        }

        // First pass with interpreted spark expressions
        Dataset<Row> interpreted = executeCalcInterpreted(ds, renamedExpressionString);

        // Execute the rest using the resolvable expressions
        Dataset<Row> evaluated = executeCalcEvaluated(interpreted, renamedExpressions);

        // Rename the columns back to their original names
        Dataset<Row> renamed = rename(evaluated, aliasesToName);

        // Create the new role map.
        var roleMap = getRoleMap(dataset);
        roleMap.putAll(roles);

        return new SparkDatasetExpression(new SparkDataset(renamed, roleMap));
    }

    private Dataset<Row> executeCalcEvaluated(Dataset<Row> interpreted, Map<String, ResolvableExpression> expressions) {
        var columnNames = Set.of(interpreted.columns());
        Column structColumns = struct(columnNames.stream().map(colName -> col(colName)).toArray(Column[]::new));
        for (var name : expressions.keySet()) {
            // Ignore the columns that already exist.
            if (columnNames.contains(name)) {
                continue;
            }
            // Execute the ResolvableExpression by wrapping it in a UserDefinedFunction.
            ResolvableExpression expression = expressions.get(name);
            UserDefinedFunction exprFunction = udf((Row row) -> {
                SparkRowMap context = new SparkRowMap(row);
                return expression.resolve(context);
            }, fromVtlType(expression.getType()));
            interpreted = interpreted.withColumn(name, exprFunction.apply(structColumns));
        }
        return interpreted;
    }

    private Dataset<Row> executeCalcInterpreted(Dataset<Row> result, Map<String, String> expressionStrings) {
        for (String name : expressionStrings.keySet()) {
            try {
                String expression = expressionStrings.get(name);
                result = result.withColumn(name, expr(expression));
            } catch (Exception ignored) {
            }
        }
        return result;
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

        var result = rename(dataset.getSparkDataset(), fromTo);

        var originalRoles = getRoleMap(dataset);
        var renamedRoles = new LinkedHashMap<>(originalRoles);
        for (Map.Entry<String, String> fromToEntry : fromTo.entrySet()) {
            renamedRoles.put(fromToEntry.getValue(), originalRoles.get(fromToEntry.getKey()));
        }

        return new SparkDatasetExpression(new SparkDataset(result, renamedRoles));
    }

    public Dataset<Row> rename(Dataset<Row> dataset, Map<String, String> fromTo) {
        List<Column> columns = new ArrayList<>();
        for (String name : dataset.columns()) {
            if (fromTo.containsKey(name)) {
                columns.add(col(name).as(fromTo.get(name)));
            } else if (!fromTo.containsValue(name)) {
                columns.add(col(name));
            }
        }
        return dataset.select(iterableAsScalaIterable(columns).toSeq());
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
    public DatasetExpression executeAggr(DatasetExpression dataset, List<String> groupBy, Map<String, AggregationExpression> collectorMap) {
        SparkDataset sparkDataset = asSparkDataset(dataset);
        List<Column> columns = collectorMap.entrySet().stream()
                .map(e -> convertAggregation(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        List<Column> groupByColumns = groupBy.stream().map(name -> col(name)).collect(Collectors.toList());
        Dataset<Row> result = sparkDataset.getSparkDataset().groupBy(iterableAsScalaIterable(groupByColumns).toSeq())
                .agg(columns.get(0), iterableAsScalaIterable(columns.subList(1, columns.size())).toSeq());
        return new SparkDatasetExpression(new SparkDataset(result));
    }

    @Override
    public DatasetExpression executeSimpleAnalytic(
            DatasetExpression dataset,
            String targetColName,
            Analytics.Function function,
            String sourceColName,
            List<String> partitionBy,
            Map<String, Analytics.Order> orderBy,
            Analytics.WindowSpec window
    ) {
        SparkDataset sparkDataset = asSparkDataset(dataset);

        //step1: build window spec
        WindowSpec windowSpec = buildWindowSpec(partitionBy, orderBy, window);

        // step 2: call analytic func on window spec
        // 2.1 get all measurement column

        Column column;
        switch (function) {
            case COUNT:
                column = count(sourceColName).over(windowSpec);
                break;
            case SUM:
                column = sum(sourceColName).over(windowSpec);
                break;
            case MIN:
                column = min(sourceColName).over(windowSpec);
                break;
            case MAX:
                column = max(sourceColName).over(windowSpec);
                break;
            case AVG:
                column = avg(sourceColName).over(windowSpec);
                break;
            case MEDIAN:
                column = percentile_approx(col(sourceColName), lit(0.5),
                        lit(DEFAULT_MEDIAN_ACCURACY)).over(windowSpec);
                break;
            case STDDEV_POP:
                column = stddev_pop(sourceColName).over(windowSpec);
                break;
            case STDDEV_SAMP:
                column = stddev_samp(sourceColName).over(windowSpec);
                break;
            case VAR_POP:
                column = var_pop(sourceColName).over(windowSpec);
                break;
            case VAR_SAMP:
                column = var_samp(sourceColName).over(windowSpec);
                break;
            case FIRST_VALUE:
                column = first(sourceColName).over(windowSpec);
                break;
            case LAST_VALUE:
                column = last(sourceColName).over(windowSpec);
                break;
            default:
                throw new UnsupportedOperationException("Unknown analytic function");

        }
        var result = sparkDataset.getSparkDataset().withColumn(targetColName, column);
        return new SparkDatasetExpression(new SparkDataset(result));
    }

    @Override
    public DatasetExpression executeLeadOrLagAn(
            DatasetExpression dataset,
            String targetColName,
            Analytics.Function function,
            String sourceColName,
            int offset,
            List<String> partitionBy,
            Map<String, Analytics.Order> orderBy) {
        SparkDataset sparkDataset = asSparkDataset(dataset);

        //step1: build window spec
        WindowSpec windowSpec = buildWindowSpec(partitionBy, orderBy);

        // step 2: call analytic func on window spec
        Column column;
        switch (function) {
            case LEAD:
                column = lead(sourceColName, offset).over(windowSpec);
                break;
            case LAG:
                column = lag(sourceColName, offset).over(windowSpec);
                break;
            default:
                throw new UnsupportedOperationException("Unknown analytic function");
        }
        var result = sparkDataset.getSparkDataset().withColumn(targetColName, column);
        return new SparkDatasetExpression(new SparkDataset(result));
    }

    @Override
    public DatasetExpression executeRatioToReportAn(
            DatasetExpression dataset,
            String targetColName,
            Analytics.Function function,
            String sourceColName,
            List<String> partitionBy) {
        if (!function.equals(Analytics.Function.RATIO_TO_REPORT))
            throw new UnsupportedOperationException("Unknown analytic function");

        SparkDataset sparkDataset = asSparkDataset(dataset);
        //step1: build window spec
        WindowSpec windowSpec = buildWindowSpec(partitionBy);

        // step 2: call analytic func on window spec
        String totalColName = "total_" + sourceColName;
        // 2.2 add the result column for the calc clause
        Dataset<Row> result = sparkDataset.getSparkDataset().withColumn(totalColName, sum(sourceColName).over(windowSpec)).
                withColumn(targetColName, col(sourceColName).divide(col(totalColName))).drop(totalColName);
        // 2.3 without the calc clause, we need to overwrite the measure columns with the result column
        return new SparkDatasetExpression(new SparkDataset(result));
    }

    @Override
    public DatasetExpression executeRankAn(
            DatasetExpression dataset,
            String targetColName,
            Analytics.Function function,
            List<String> partitionBy,
            Map<String, Analytics.Order> orderBy) {
        if (!function.equals(Analytics.Function.RANK))
            throw new UnsupportedOperationException("Unknown analytic function");

        SparkDataset sparkDataset = asSparkDataset(dataset);
        //step1: build window spec
        WindowSpec windowSpec = buildWindowSpec(partitionBy, orderBy);

        // step 2: call analytic func on window spec
        Dataset<Row> result = sparkDataset.getSparkDataset().withColumn(targetColName, rank().over(windowSpec));
        // 2.3 without the calc clause, we need to overwrite the measure columns with the result column
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

    /**
     * Utility method used for the implementation of the different types of join operations.
     *
     * @param sparkDatasets a list datasets.
     * @param identifiers   the list of identifiers to join on.
     * @param type          the type of join operation.
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

package fr.insee.vtl.spark;

import static fr.insee.vtl.model.AggregationExpression.*;
import static fr.insee.vtl.model.Dataset.Component;
import static fr.insee.vtl.model.Dataset.Role;
import static fr.insee.vtl.model.Dataset.Role.IDENTIFIER;
import static fr.insee.vtl.spark.SparkDataset.fromVtlType;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;
import static scala.collection.JavaConverters.iterableAsScalaIterable;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.semantics.validation.HierarchicalValidationRuntime;
import fr.insee.vtl.model.*;
import java.util.*;
import java.util.stream.Collectors;
import javax.script.ScriptEngine;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * The <code>SparkProcessingEngine</code> class is an implementation of a VTL engine using Apache
 * Spark.
 */
public class SparkProcessingEngine implements ProcessingEngine, HierarchicalValidationRuntime {

  public static final Integer DEFAULT_MEDIAN_ACCURACY = 1000000;
  public static final UnsupportedOperationException UNKNOWN_ANALYTIC_FUNCTION =
      new UnsupportedOperationException("Unknown analytic function");
  private final SparkSession spark;

  /**
   * Constructor taking an existing Spark session.
   *
   * @param spark The Spark session to use for the engine.
   */
  public SparkProcessingEngine(SparkSession spark) {
    spark.conf().set("spark.sql.datetime.java8API.enabled", true);
    this.spark = Objects.requireNonNull(spark);
  }

  private static Map<String, Role> getRoleMap(Collection<Component> components) {
    return components.stream().collect(Collectors.toMap(Component::getName, Component::getRole));
  }

  private static Map<String, Role> getRoleMap(fr.insee.vtl.model.Dataset dataset) {
    return getRoleMap(dataset.getDataStructure().values());
  }

  private static Map<String, Role> getRoleMap(
      Structured.DataStructure structure, List<String> columnNames) {
    Map<String, Role> roles = new LinkedHashMap<>();
    for (String name : columnNames) {
      Component component = structure.get(name);
      if (component != null) {
        roles.put(name, component.getRole());
      }
    }
    return roles;
  }

  // TODO (expression instanceof MinAggregationExpression)
  // TODO column = stddev_pop(columnName);
  private static Column convertAggregation(String columnName, AggregationExpression expression)
      throws UnsupportedOperationException {
    Column column;
    if (expression instanceof MinAggregationExpression) {
      column = min(SparkUtils.safeCol(columnName));
    } else if (expression instanceof MaxAggregationExpression) {
      column = max(SparkUtils.safeCol(columnName));
    } else if (expression instanceof AverageAggregationExpression) {
      column = avg(SparkUtils.safeCol(columnName));
    } else if (expression instanceof SumAggregationExpression) {
      column = sum(SparkUtils.safeCol(columnName));
    } else if (expression instanceof CountAggregationExpression) {
      column = count("*");
    } else if (expression instanceof MedianAggregationExpression) {
      column =
          percentile_approx(SparkUtils.safeCol(columnName), lit(0.5), lit(DEFAULT_MEDIAN_ACCURACY));
    } else if (expression instanceof StdDevPopAggregationExpression) {
      column = stddev_pop(SparkUtils.safeCol(columnName));
    } else if (expression instanceof StdDevSampAggregationExpression) {
      column = stddev_samp(SparkUtils.safeCol(columnName));
    } else if (expression instanceof VarPopAggregationExpression) {
      column = var_pop(SparkUtils.safeCol(columnName));
    } else if (expression instanceof VarSampAggregationExpression) {
      column = var_samp(SparkUtils.safeCol(columnName));
    } else {
      throw new UnsupportedOperationException("unknown aggregation " + expression.getClass());
    }
    return column.alias(columnName);
  }

  //    todo need to add unit test
  private static WindowSpec buildWindowSpec(List<String> partitionBy) {
    return buildWindowSpec(partitionBy, null, null);
  }

  //    todo need to add unit test
  private static WindowSpec buildWindowSpec(
      List<String> partitionBy, Map<String, Analytics.Order> orderBy) {
    return buildWindowSpec(partitionBy, orderBy, null);
  }

  //    todo need to add unit test
  private static WindowSpec buildWindowSpec(
      List<String> partitionBy, Map<String, Analytics.Order> orderBy, Analytics.WindowSpec window) {
    if (partitionBy == null) {
      partitionBy = List.of();
    }

    Column[] partitionCols =
        scala.collection.JavaConverters.seqAsJavaList(colNameToCol(partitionBy))
            .toArray(new Column[0]);

    WindowSpec windowSpec = Window.partitionBy(partitionCols);

    if (orderBy == null) {
      orderBy = Map.of();
    }

    Column[] orderCols =
        scala.collection.JavaConverters.seqAsJavaList(buildOrderCol(orderBy))
            .toArray(new Column[0]);
    windowSpec = windowSpec.orderBy(orderCols);

    if (window instanceof Analytics.DataPointWindow) {
      windowSpec = windowSpec.rowsBetween(-window.getLower(), window.getUpper());
    } else if (window instanceof Analytics.RangeWindow) {
      windowSpec = windowSpec.rangeBetween(-window.getLower(), window.getUpper());
    }

    return windowSpec;
  }

  public static Seq<Column> colNameToCol(List<String> inputColNames) {
    List<Column> cols = new ArrayList<>();
    for (String colName : inputColNames) {
      cols.add(SparkUtils.safeCol(colName));
    }
    return JavaConverters.asScalaIteratorConverter(cols.iterator()).asScala().toSeq();
  }

  // helper function that builds order col expression with asc and desc spec
  public static Seq<Column> buildOrderCol(Map<String, Analytics.Order> orderCols) {
    List<Column> orders = new ArrayList<>();
    for (Map.Entry<String, Analytics.Order> entry : orderCols.entrySet()) {
      if (entry.getValue().equals(Analytics.Order.DESC)) {
        orders.add(SparkUtils.safeCol(entry.getKey()).desc());
      } else {
        orders.add(SparkUtils.safeCol(entry.getKey()));
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
    if (expression instanceof SparkDatasetExpression datasetExpression) {
      return datasetExpression.resolve(Map.of());
    } else {
      var dataset = expression.resolve(Map.of());
      if (dataset instanceof PersistentDataset persistentDataset) {
        dataset = persistentDataset.getDelegate();
      }
      if (dataset instanceof SparkDataset sparkDataset) {
        return sparkDataset;
      } else {
        return new SparkDataset(dataset, getRoleMap(dataset), spark);
      }
    }
  }

  @Override
  public DatasetExpression executeCalc(
      DatasetExpression expression,
      Map<String, ResolvableExpression> expressions,
      Map<String, Role> roles,
      Map<String, String> expressionStrings) {
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

    return new SparkDatasetExpression(new SparkDataset(renamed, roleMap), expression);
  }

  private Dataset<Row> executeCalcEvaluated(
      Dataset<Row> interpreted, Map<String, ResolvableExpression> expressions) {
    var columnNames = Set.of(interpreted.columns());
    Column structColumns =
        struct(columnNames.stream().map(colName -> col(colName)).toArray(Column[]::new));
    for (var name : expressions.keySet()) {
      // Ignore the columns that already exist.
      if (columnNames.contains(name)) {
        continue;
      }
      // Execute the ResolvableExpression by wrapping it in a UserDefinedFunction.
      ResolvableExpression expression = expressions.get(name);
      UserDefinedFunction exprFunction =
          udf(
              (Row row) -> {
                try {
                  SparkRowMap context = new SparkRowMap(row);
                  Object result = expression.resolve(context);
                  // Convert java.time.Instant to java.sql.Date for Spark compatibility
                  if (result instanceof java.time.Instant instant) {
                    return java.sql.Date.valueOf(
                        instant.atZone(java.time.ZoneOffset.UTC).toLocalDate());
                  }
                  return result;
                } catch (VtlRuntimeException e) {
                  // VtlRuntimeException already wraps the real VTL error, re-throw it
                  throw e;
                } catch (Exception e) {
                  // Wrap any other exception to provide context
                  throw new RuntimeException(
                      "Error in UDF for column '" + name + "': " + e.getMessage(), e);
                }
              },
              fromVtlType(expression.getType()));
      interpreted = interpreted.withColumn(name, exprFunction.apply(structColumns));
    }
    return interpreted;
  }

  private Dataset<Row> executeCalcInterpreted(
      Dataset<Row> result, Map<String, String> expressionStrings) {
    for (String name : expressionStrings.keySet()) {
      try {
        String expression = expressionStrings.get(name);
        if (expression == null) continue;
        result = result.withColumn(name, expr(expression));
      } catch (Exception e) {
        // Silently ignore expressions that Spark SQL cannot interpret directly.
        // These will be evaluated as ResolvableExpressions in executeCalcEvaluated instead.
      }
    }
    return result;
  }

  @Override
  public DatasetExpression executeFilter(
      DatasetExpression expression, ResolvableExpression filter, String filterText) {
    Objects.requireNonNull(filter, "filter");
    SparkDataset dataset = asSparkDataset(expression);

    Dataset<Row> ds = dataset.getSparkDataset();
    try {
      Dataset<Row> result = ds.filter(filterText);
      return new SparkDatasetExpression(new SparkDataset(result, getRoleMap(dataset)), expression);
    } catch (Exception e) {
      SparkFilterFunction filterFunction = new SparkFilterFunction(filter);
      Dataset<Row> result = ds.filter(filterFunction);
      return new SparkDatasetExpression(new SparkDataset(result, getRoleMap(dataset)), expression);
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

    return new SparkDatasetExpression(new SparkDataset(result, renamedRoles), expression);
  }

  public Dataset<Row> rename(Dataset<Row> dataset, Map<String, String> fromTo) {
    List<Column> columns = new ArrayList<>();
    for (String name : dataset.columns()) {
      if (fromTo.containsKey(name)) {
        columns.add(SparkUtils.safeCol(name).as(fromTo.get(name)));
      } else if (!fromTo.containsValue(name)) {
        columns.add(SparkUtils.safeCol(name));
      }
    }
    return dataset.select(iterableAsScalaIterable(columns).toSeq());
  }

  @Override
  public DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames) {
    SparkDataset dataset = asSparkDataset(expression);
    org.apache.spark.sql.Dataset<Row> sparkDataset = dataset.getSparkDataset();

    List<Column> columns =
        columnNames.stream()
            .map(name -> SparkUtils.safeCol(name).as(name))
            .collect(Collectors.toList());
    Column[] columnArray = columns.toArray(new Column[0]);

    Dataset<Row> result = sparkDataset.select(columnArray);

    return new SparkDatasetExpression(
        new SparkDataset(result, getRoleMap(expression.getDataStructure(), columnNames)),
        expression);
  }

  private boolean checkColNameCompatibility(List<DatasetExpression> datasets) {
    boolean result = true;
    IndexedHashMap<String, Component> baseStructure = datasets.get(0).getDataStructure();
    for (int i = 1; i <= datasets.size() - 1; i++) {
      // check if current structure equals base structure
      IndexedHashMap<String, Component> curretStructure = datasets.get(i).getDataStructure();
      if (!baseStructure.equals(curretStructure)) {
        result = false;
        break;
      }
    }
    return result;
  }

  @Override
  public DatasetExpression executeUnion(
      List<DatasetExpression> datasets, List<String> dedupeOnColumns) {
    if (!checkColNameCompatibility(datasets)) {
      throw new UnsupportedOperationException("The schema of the dataset is not compatible");
    }
    Structured.DataStructure baseDataStructure = datasets.get(0).getDataStructure();
    Map<String, Role> dataRoles = new LinkedHashMap<>();
    for (String key : baseDataStructure.keySet()) {
      Component item = baseDataStructure.get(key);
      dataRoles.put(item.getName(), item.getRole());
    }

    if (datasets.size() == 1) {
      return datasets.get(0);
    }

    Dataset<Row> result = asSparkDataset(datasets.get(0)).getSparkDataset();
    for (int i = 1; i < datasets.size(); i++) {
      result = result.union(asSparkDataset(datasets.get(i)).getSparkDataset());
    }
    if (!dedupeOnColumns.isEmpty()) {
      result = result.dropDuplicates(dedupeOnColumns.toArray(new String[0]));
    }
    return new SparkDatasetExpression(new SparkDataset(result, dataRoles), datasets.get(0));
  }

  @Override
  public DatasetExpression executeAggr(
      DatasetExpression dataset,
      List<String> groupBy,
      Map<String, AggregationExpression> collectorMap) {
    SparkDataset sparkDataset = asSparkDataset(dataset);
    Structured.DataStructure outputStructure =
        AggregationOutputStructure.mechanical(dataset.getDataStructure(), groupBy, collectorMap);
    List<Column> columns =
        collectorMap.entrySet().stream()
            .map(e -> convertAggregation(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    List<Column> groupByColumns =
        groupBy.stream().map(SparkUtils::safeCol).collect(Collectors.toList());
    RelationalGroupedDataset grouped =
        sparkDataset.getSparkDataset().groupBy(iterableAsScalaIterable(groupByColumns).toSeq());
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("aggregation requires at least one aggregate expression");
    }
    Dataset<Row> result =
        grouped.agg(
            columns.get(0), iterableAsScalaIterable(columns.subList(1, columns.size())).toSeq());
    SparkDataset sparkDs = new SparkDataset(result, outputStructure);
    return new SparkDatasetExpression(sparkDs, dataset);
  }

  @Override
  public DatasetExpression executeSimpleAnalytic(
      DatasetExpression dataset,
      String targetColName,
      Analytics.Function function,
      String sourceColName,
      List<String> partitionBy,
      Map<String, Analytics.Order> orderBy,
      Analytics.WindowSpec window) {
    SparkDataset sparkDataset = asSparkDataset(dataset);

    // step1: build window spec
    WindowSpec windowSpec = buildWindowSpec(partitionBy, orderBy, window);

    // step 2: call analytic func on window spec
    // 2.1 get all measurement column

    Column safeCol = SparkUtils.safeCol(sourceColName);

    Column column =
        switch (function) {
          case COUNT -> count(safeCol).over(windowSpec);
          case SUM -> sum(safeCol).over(windowSpec);
          case MIN -> min(safeCol).over(windowSpec);
          case MAX -> max(safeCol).over(windowSpec);
          case AVG -> avg(safeCol).over(windowSpec);
          case MEDIAN ->
              percentile_approx(safeCol, lit(0.5), lit(DEFAULT_MEDIAN_ACCURACY)).over(windowSpec);
          case STDDEV_POP -> stddev_pop(safeCol).over(windowSpec);
          case STDDEV_SAMP -> stddev_samp(safeCol).over(windowSpec);
          case VAR_POP -> var_pop(safeCol).over(windowSpec);
          case VAR_SAMP -> var_samp(safeCol).over(windowSpec);
          case FIRST_VALUE -> first(safeCol).over(windowSpec);
          case LAST_VALUE -> last(safeCol).over(windowSpec);
          default -> throw UNKNOWN_ANALYTIC_FUNCTION;
        };
    var result = sparkDataset.getSparkDataset().withColumn(targetColName, column);
    return new SparkDatasetExpression(new SparkDataset(result), dataset);
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

    // step1: build window spec
    WindowSpec windowSpec = buildWindowSpec(partitionBy, orderBy);

    // step 2: call analytic func on window spec
    Column column =
        switch (function) {
          case LEAD -> lead(sourceColName, offset).over(windowSpec);
          case LAG -> lag(sourceColName, offset).over(windowSpec);
          default -> throw UNKNOWN_ANALYTIC_FUNCTION;
        };
    var result = sparkDataset.getSparkDataset().withColumn(targetColName, column);
    return new SparkDatasetExpression(new SparkDataset(result), dataset);
  }

  @Override
  public DatasetExpression executeRatioToReportAn(
      DatasetExpression dataset,
      String targetColName,
      Analytics.Function function,
      String sourceColName,
      List<String> partitionBy) {
    if (!function.equals(Analytics.Function.RATIO_TO_REPORT)) throw UNKNOWN_ANALYTIC_FUNCTION;

    SparkDataset sparkDataset = asSparkDataset(dataset);
    // step1: build window spec
    WindowSpec windowSpec = buildWindowSpec(partitionBy);

    // step 2: call analytic func on window spec
    String totalColName = "total_" + sourceColName;
    // 2.2 add the result column for the calc clause
    Dataset<Row> result =
        sparkDataset
            .getSparkDataset()
            .withColumn(totalColName, sum(SparkUtils.safeCol(sourceColName)).over(windowSpec))
            .withColumn(targetColName, SparkUtils.safeCol(sourceColName).divide(col(totalColName)))
            .drop(totalColName);
    // 2.3 without the calc clause, we need to overwrite the measure columns with the result column
    return new SparkDatasetExpression(new SparkDataset(result), dataset);
  }

  @Override
  public DatasetExpression executeRankAn(
      DatasetExpression dataset,
      String targetColName,
      Analytics.Function function,
      List<String> partitionBy,
      Map<String, Analytics.Order> orderBy) {
    if (!function.equals(Analytics.Function.RANK)) throw UNKNOWN_ANALYTIC_FUNCTION;

    SparkDataset sparkDataset = asSparkDataset(dataset);
    // step1: build window spec
    WindowSpec windowSpec = buildWindowSpec(partitionBy, orderBy);

    // step 2: call analytic func on window spec
    Dataset<Row> result =
        sparkDataset.getSparkDataset().withColumn(targetColName, rank().over(windowSpec));
    // 2.3 without the calc clause, we need to overwrite the measure columns with the result column
    return new SparkDatasetExpression(new SparkDataset(result), dataset);
  }

  @Override
  public DatasetExpression executeInnerJoin(
      Map<String, DatasetExpression> datasets, List<Component> components) {
    return joinDatasets(datasets, components, "inner");
  }

  @Override
  public DatasetExpression executeLeftJoin(
      Map<String, DatasetExpression> datasets, List<Structured.Component> components) {
    return joinDatasets(datasets, components, "left");
  }

  @Override
  public DatasetExpression executeCrossJoin(
      Map<String, DatasetExpression> datasets, List<Component> identifiers) {
    return joinDatasets(datasets, List.of(), "cross");
  }

  @Override
  public DatasetExpression executeFullJoin(
      Map<String, DatasetExpression> datasets, List<Component> identifiers) {
    return joinDatasets(datasets, identifiers, "outer");
  }

  private DatasetExpression joinDatasets(
      Map<String, DatasetExpression> datasets, List<Component> joinKeys, String joinType) {
    List<Dataset<Row>> sparkDatasets = toAliasedDatasets(datasets);
    List<String> identifiers = identifierNames(joinKeys);
    Dataset<Row> joined = executeJoin(sparkDatasets, identifiers, joinType);
    Map<String, Role> roles = mergeJoinRoles(datasets, joined);
    DatasetExpression datasetExpression = datasets.entrySet().iterator().next().getValue();
    return new SparkDatasetExpression(new SparkDataset(joined, roles), datasetExpression);
  }

  private Map<String, Role> mergeJoinRoles(
      Map<String, DatasetExpression> datasets, Dataset<Row> joined) {
    Map<String, Role> roles = new LinkedHashMap<>();
    for (DatasetExpression expression : datasets.values()) {
      getRoleMap(asSparkDataset(expression)).forEach(roles::putIfAbsent);
    }
    Set<String> columns = Set.of(joined.columns());
    roles.keySet().retainAll(columns);
    return roles;
  }

  @Override
  public Map<String, Object> columnBindings(
      DatasetExpression dataset, String keyColumn, String valueColumn) {
    SparkDataset sparkDataset = asSparkDataset(dataset);
    return SparkDataset.columnBindingsMap(sparkDataset.getSparkDataset(), keyColumn, valueColumn);
  }

  @Override
  public DatasetExpression filterKeepingSchema(DatasetExpression expression, String filterText) {
    SparkDataset dataset = asSparkDataset(expression);
    Dataset<Row> ds = dataset.getSparkDataset();
    try {
      Dataset<Row> result = ds.filter(filterText);
      if (result.isEmpty()) {
        result = ds.limit(1);
      }
      return new SparkDatasetExpression(new SparkDataset(result, getRoleMap(dataset)), expression);
    } catch (Exception e) {
      return executeFilter(
          expression, ResolvableExpression.withType(Boolean.class).using(c -> null), filterText);
    }
  }

  private List<Dataset<Row>> toAliasedDatasets(Map<String, DatasetExpression> datasets) {
    List<Dataset<Row>> sparkDatasets = new ArrayList<>();
    for (Map.Entry<String, DatasetExpression> dataset : datasets.entrySet()) {
      var sparkDataset = asSparkDataset(dataset.getValue()).getSparkDataset().as(dataset.getKey());
      sparkDatasets.add(sparkDataset);
    }
    return sparkDatasets;
  }

  /**
   * Utility method used for the implementation of the different types of join operations.
   *
   * @param sparkDatasets a list datasets.
   * @param identifiers the list of identifiers to join on.
   * @param type the type of join operation.
   * @return The dataset resulting from the join operation.
   */
  public Dataset<Row> executeJoin(
      List<Dataset<Row>> sparkDatasets, List<String> identifiers, String type) {
    var iterator = sparkDatasets.iterator();
    var result = iterator.next();
    while (iterator.hasNext()) {
      if (type.equals("cross")) result = result.crossJoin(iterator.next());
      else
        result = result.join(iterator.next(), iterableAsScalaIterable(identifiers).toSeq(), type);
    }
    return result;
  }

  /**
   * Execute pivot on dataset expression.
   *
   * @param dsExpr dataset expression
   * @param idName identifier name
   * @param meName measure name
   * @param pos script error position
   * @return the result of the pivot
   */
  public DatasetExpression executePivot(
      DatasetExpression dsExpr, String idName, String meName, Positioned pos) {

    Dataset<Row> sparkDataset = asSparkDataset(dsExpr).getSparkDataset();

    List<String> groupByIdentifiers = new ArrayList<>(dsExpr.getIdentifierNames());
    groupByIdentifiers.remove(idName);

    Column[] groupByCols =
        groupByIdentifiers.stream().map(SparkUtils::safeCol).toArray(Column[]::new);

    // TODO: fail if any values needs to be aggregated
    Dataset<Row> result =
        sparkDataset
            .groupBy(groupByCols)
            .pivot(SparkUtils.safeCol(idName))
            .agg(functions.first(meName));

    return new SparkDatasetExpression(new SparkDataset(result), pos);
  }

  /**
   * The <code>Factory</code> class is an implementation of a VTL engine factory that returns Spark
   * engines.
   */
  public static class Factory implements ProcessingEngineFactory {

    private static final String SPARK_SESSION = "$vtl.spark.session";

    @Override
    public String getName() {
      return "spark4";
    }

    @Override
    public ProcessingEngine getProcessingEngine(ScriptEngine engine) {
      // Try to find the session in the script engine.
      var session = engine.get(SPARK_SESSION);
      if (session != null) {
        if (session instanceof SparkSession sparkSession) {
          return new SparkProcessingEngine(sparkSession);
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

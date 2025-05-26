package fr.insee.vtl.spark;

import static fr.insee.vtl.model.AggregationExpression.*;
import static fr.insee.vtl.model.Dataset.Component;
import static fr.insee.vtl.model.Dataset.Role;
import static fr.insee.vtl.model.Dataset.Role.IDENTIFIER;
import static fr.insee.vtl.model.Dataset.Role.MEASURE;
import static fr.insee.vtl.spark.SparkDataset.fromVtlType;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;
import static scala.collection.JavaConverters.iterableAsScalaIterable;

import fr.insee.vtl.model.*;
import java.util.*;
import java.util.stream.Collectors;
import javax.script.ScriptEngine;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * The <code>SparkProcessingEngine</code> class is an implementation of a VTL engine using Apache
 * Spark.
 */
public class SparkProcessingEngine implements ProcessingEngine {

  public static final Integer DEFAULT_MEDIAN_ACCURACY = 1000000;
  public static final UnsupportedOperationException UNKNOWN_ANALYTIC_FUNCTION =
      new UnsupportedOperationException("Unknown analytic function");
  private static final String BOOLVAR = "bool_var";
  private static final String ERRORCODE = "errorcode";
  private static final String ERRORLEVEL = "errorlevel";
  private static final String RULEID = "ruleid";
  private static final String IMBALANCE = "imbalance";
  private static final String NON_NULL = "non_null";
  private static final String NON_ZERO = "non_zero";
  private static final String PARTIAL_NULL = "partial_null";
  private static final String PARTIAL_ZERO = "partial_zero";
  private static final String ALWAYS_NULL = "always_null";
  private static final String ALWAYS_ZERO = "always_zero";
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

  // TODO (expression instanceof MinAggregationExpression)
  // TODO column = stddev_pop(columnName);
  private static Column convertAggregation(String columnName, AggregationExpression expression)
      throws UnsupportedOperationException {
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

    WindowSpec windowSpec = Window.partitionBy(colNameToCol(partitionBy));

    if (orderBy == null) {
      orderBy = Map.of();
    }
    windowSpec = windowSpec.orderBy(buildOrderCol(orderBy));

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
    if (expression instanceof SparkDatasetExpression datasetExpression) {
      return datasetExpression.resolve(Map.of());
    } else {
      var dataset = expression.resolve(Map.of());
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
      try {
        UserDefinedFunction exprFunction =
            udf(
                (Row row) -> {
                  SparkRowMap context = new SparkRowMap(row);
                  return expression.resolve(context);
                },
                fromVtlType(expression.getType()));
        interpreted = interpreted.withColumn(name, exprFunction.apply(structColumns));
      } catch (Exception e) {
        System.out.println(name);
      }
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
      } catch (Exception ignored) {
      }
    }
    return result;
  }

  @Override
  public DatasetExpression executeFilter(
      DatasetExpression expression, ResolvableExpression filter, String filterText) {
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

    return new SparkDatasetExpression(new SparkDataset(result, getRoleMap(dataset)), expression);
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
  public DatasetExpression executeUnion(List<DatasetExpression> datasets) {
    DatasetExpression dataset = datasets.get(0);

    if (!checkColNameCompatibility(datasets))
      throw new UnsupportedOperationException("The schema of the dataset is not compatible");
    // use the base data structure to build the result data roles
    Structured.DataStructure baseDataStructure = datasets.get(0).getDataStructure();
    Set<String> keys = baseDataStructure.keySet();
    HashMap<String, Role> dataRoles = new HashMap<>();
    for (String key : keys) {
      Component item = baseDataStructure.get(key);
      dataRoles.put(item.getName(), item.getRole());
    }

    // get Id column list
    List<String> colNames = datasets.get(0).getColumnNames();
    ArrayList<String> idColList = new ArrayList<>();
    IndexedHashMap<String, Component> structure = dataset.getDataStructure();
    // get column list with ID role, it will be used to drop duplicated rows
    for (String colName : colNames) {
      if (structure.get(colName).getRole().equals(IDENTIFIER)) idColList.add(colName);
    }
    int size = datasets.size();

    if (size == 1) {
      return datasets.get(0);
    } else {
      Dataset<Row> result = asSparkDataset(datasets.get(0)).getSparkDataset();
      for (int i = 1; i <= size - 1; i++) {
        Dataset<Row> current = asSparkDataset(datasets.get(i)).getSparkDataset();
        result = result.union(current);
      }
      result = result.dropDuplicates(iterableAsScalaIterable(idColList).toSeq());
      return new SparkDatasetExpression(new SparkDataset(result, dataRoles), datasets.get(0));
    }
  }

  @Override
  public DatasetExpression executeAggr(
      DatasetExpression dataset,
      List<String> groupBy,
      Map<String, AggregationExpression> collectorMap) {
    SparkDataset sparkDataset = asSparkDataset(dataset);
    List<Column> columns =
        collectorMap.entrySet().stream()
            .map(e -> convertAggregation(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    List<Column> groupByColumns =
        groupBy.stream().map(name -> col(name)).collect(Collectors.toList());
    Dataset<Row> result =
        sparkDataset
            .getSparkDataset()
            .groupBy(iterableAsScalaIterable(groupByColumns).toSeq())
            .agg(
                columns.get(0),
                iterableAsScalaIterable(columns.subList(1, columns.size())).toSeq());
    SparkDataset sparkDs = new SparkDataset(result, dataset.getRoles());
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

    Column column =
        switch (function) {
          case COUNT -> count(sourceColName).over(windowSpec);
          case SUM -> sum(sourceColName).over(windowSpec);
          case MIN -> min(sourceColName).over(windowSpec);
          case MAX -> max(sourceColName).over(windowSpec);
          case AVG -> avg(sourceColName).over(windowSpec);
          case MEDIAN ->
              percentile_approx(col(sourceColName), lit(0.5), lit(DEFAULT_MEDIAN_ACCURACY))
                  .over(windowSpec);
          case STDDEV_POP -> stddev_pop(sourceColName).over(windowSpec);
          case STDDEV_SAMP -> stddev_samp(sourceColName).over(windowSpec);
          case VAR_POP -> var_pop(sourceColName).over(windowSpec);
          case VAR_SAMP -> var_samp(sourceColName).over(windowSpec);
          case FIRST_VALUE -> first(sourceColName).over(windowSpec);
          case LAST_VALUE -> last(sourceColName).over(windowSpec);
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
            .withColumn(totalColName, sum(sourceColName).over(windowSpec))
            .withColumn(targetColName, col(sourceColName).divide(col(totalColName)))
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
    List<Dataset<Row>> sparkDatasets = toAliasedDatasets(datasets);
    List<String> identifiers = identifierNames(components);
    var innerJoin = executeJoin(sparkDatasets, identifiers, "inner");
    DatasetExpression datasetExpression = datasets.entrySet().iterator().next().getValue();
    return new SparkDatasetExpression(
        new SparkDataset(innerJoin, getRoleMap(components)), datasetExpression);
  }

  @Override
  public DatasetExpression executeLeftJoin(
      Map<String, DatasetExpression> datasets, List<Structured.Component> components) {
    List<Dataset<Row>> sparkDatasets = toAliasedDatasets(datasets);
    List<String> identifiers = identifierNames(components);
    var innerJoin = executeJoin(sparkDatasets, identifiers, "left");
    DatasetExpression datasetExpression = datasets.entrySet().iterator().next().getValue();
    return new SparkDatasetExpression(
        new SparkDataset(innerJoin, getRoleMap(components)), datasetExpression);
  }

  @Override
  public DatasetExpression executeCrossJoin(
      Map<String, DatasetExpression> datasets, List<Component> identifiers) {
    List<Dataset<Row>> sparkDatasets = toAliasedDatasets(datasets);
    var crossJoin = executeJoin(sparkDatasets, List.of(), "cross");
    DatasetExpression datasetExpression = datasets.entrySet().iterator().next().getValue();
    return new SparkDatasetExpression(
        new SparkDataset(crossJoin, getRoleMap(identifiers)), datasetExpression);
  }

  @Override
  public DatasetExpression executeFullJoin(
      Map<String, DatasetExpression> datasets, List<Component> identifiers) {
    List<Dataset<Row>> sparkDatasets = toAliasedDatasets(datasets);
    List<String> identifierNames = identifierNames(identifiers);
    var crossJoin = executeJoin(sparkDatasets, identifierNames, "outer");
    DatasetExpression datasetExpression = datasets.entrySet().iterator().next().getValue();
    return new SparkDatasetExpression(
        new SparkDataset(crossJoin, getRoleMap(identifiers)), datasetExpression);
  }

  @Override
  public DatasetExpression executeValidateDPruleset(
      DataPointRuleset dpr,
      DatasetExpression dataset,
      String output,
      Positioned pos,
      List<String> toDrop) {
    SparkDataset sparkDataset = asSparkDataset(dataset);
    Dataset<Row> ds = sparkDataset.getSparkDataset();
    Dataset<Row> renamedDs = rename(ds, dpr.getAlias());

    SparkDataset sparkDs = new SparkDataset(renamedDs);
    DatasetExpression sparkDsExpr = new SparkDatasetExpression(sparkDs, pos);
    Structured.DataStructure dataStructure = sparkDs.getDataStructure();

    var roleMap = getRoleMap(sparkDataset);
    roleMap.put(RULEID, IDENTIFIER);
    roleMap.put(BOOLVAR, MEASURE);
    roleMap.put(ERRORLEVEL, MEASURE);
    roleMap.put(ERRORCODE, MEASURE);

    Class errorCodeType = dpr.getErrorCodeType();
    Class errorLevelType = dpr.getErrorLevelType();

    List<DatasetExpression> datasetsExpression =
        dpr.getRules().stream()
            .map(
                rule -> {
                  String ruleName = rule.getName();
                  ResolvableExpression ruleIdExpression =
                      ResolvableExpression.withType(String.class)
                          .withPosition(pos)
                          .using(context -> ruleName);

                  ResolvableExpression antecedentExpression =
                      rule.getBuildAntecedentExpression(dataStructure);
                  ResolvableExpression consequentExpression =
                      rule.getBuildConsequentExpression(dataStructure);

                  ResolvableExpression errorCodeExpr = rule.getErrorCodeExpression();
                  ResolvableExpression errorCodeExpression =
                      ResolvableExpression.withType(errorCodeType)
                          .withPosition(pos)
                          .using(
                              context -> {
                                if (errorCodeExpr == null) return null;
                                Map<String, Object> mapContext = (Map<String, Object>) context;
                                Object erCode = errorCodeExpr.resolve(mapContext);
                                if (erCode == null) return null;
                                Boolean antecedentValue =
                                    (Boolean) antecedentExpression.resolve(mapContext);
                                Boolean consequentValue =
                                    (Boolean) consequentExpression.resolve(mapContext);
                                return Boolean.TRUE.equals(antecedentValue)
                                        && Boolean.FALSE.equals(consequentValue)
                                    ? errorCodeType.cast(erCode)
                                    : null;
                              });

                  ResolvableExpression errorLevelExpr = rule.getErrorLevelExpression();
                  ResolvableExpression errorLevelExpression =
                      ResolvableExpression.withType(errorLevelType)
                          .withPosition(pos)
                          .using(
                              context -> {
                                if (errorLevelExpr == null) return null;
                                Map<String, Object> mapContext = (Map<String, Object>) context;
                                Object erLevel = errorLevelExpr.resolve(mapContext);
                                if (erLevel == null) return null;
                                Boolean antecedentValue =
                                    (Boolean) antecedentExpression.resolve(mapContext);
                                Boolean consequentValue =
                                    (Boolean) consequentExpression.resolve(mapContext);
                                return Boolean.TRUE.equals(antecedentValue)
                                        && Boolean.FALSE.equals(consequentValue)
                                    ? errorLevelType.cast(erLevel)
                                    : null;
                              });

                  ResolvableExpression BOOLVARExpression =
                      ResolvableExpression.withType(Boolean.class)
                          .withPosition(pos)
                          .using(
                              context -> {
                                Boolean antecedentValue =
                                    (Boolean) antecedentExpression.resolve(context);
                                Boolean consequentValue =
                                    (Boolean) consequentExpression.resolve(context);
                                if (antecedentValue == null) return consequentValue;
                                if (consequentValue == null) return antecedentValue;
                                return !antecedentValue || consequentValue;
                              });

                  Map<String, ResolvableExpression> resolvableExpressions = new HashMap<>();
                  resolvableExpressions.put(RULEID, ruleIdExpression);
                  resolvableExpressions.put(BOOLVAR, BOOLVARExpression);
                  resolvableExpressions.put(ERRORLEVEL, errorLevelExpression);
                  resolvableExpressions.put(ERRORCODE, errorCodeExpression);
                  // do we need to use execute executeCalcInterpreted too?
                  return executeCalc(sparkDsExpr, resolvableExpressions, roleMap, Map.of());
                })
            .collect(Collectors.toList());

    Dataset<Row> invertRenamedSparkDs =
        rename(
            asSparkDataset(executeUnion(datasetsExpression)).getSparkDataset(),
            invertMap(dpr.getAlias()));
    SparkDatasetExpression sparkDatasetExpression =
        new SparkDatasetExpression(new SparkDataset(invertRenamedSparkDs), pos);
    List<String> toKeep =
        sparkDatasetExpression.getColumnNames().stream()
            .filter(v -> !toDrop.contains(v))
            .collect(Collectors.toList());
    DatasetExpression cleanedExpression = executeProject(sparkDatasetExpression, toKeep);
    if (output == null || output.equals(ValidationOutput.INVALID.value)) {
      ResolvableExpression defaultExpression =
          ResolvableExpression.withType(Boolean.class).withPosition(pos).using(c -> null);
      DatasetExpression filteredDataset =
          executeFilter(cleanedExpression, defaultExpression, BOOLVAR + " = false");
      Dataset<Row> result = asSparkDataset(filteredDataset).getSparkDataset().drop(BOOLVAR);
      return new SparkDatasetExpression(new SparkDataset(result), pos);
    }
    return cleanedExpression;
  }

  @Override
  public DatasetExpression executeValidationSimple(
      DatasetExpression dsExpr,
      ResolvableExpression errorCodeExpr,
      ResolvableExpression errorLevelExpr,
      DatasetExpression imbalanceExpr,
      String output,
      Positioned pos) {
    // Rename imbalance single measure to imbalance
    SparkDataset sparkImbalanceDataset = asSparkDataset(imbalanceExpr);
    Dataset<Row> sparkImbalanceDatasetRow = sparkImbalanceDataset.getSparkDataset();
    String imbalanceMonomeasureName =
        imbalanceExpr.getDataStructure().values().stream()
            .filter(Component::isMeasure)
            .map(Component::getName)
            .collect(Collectors.toList())
            .get(0);
    Map<String, String> varsToRename =
        Map.ofEntries(Map.entry(imbalanceMonomeasureName, IMBALANCE));
    Dataset<Row> renamed = rename(sparkImbalanceDatasetRow, varsToRename);
    var imbalanceRoleMap = getRoleMap(sparkImbalanceDataset);
    SparkDatasetExpression imbalanceRenamedExpr =
        new SparkDatasetExpression(new SparkDataset(renamed, imbalanceRoleMap), pos);
    // Join expr ds & imbalance ds
    Map<String, DatasetExpression> datasetExpressions =
        Map.ofEntries(
            Map.entry("dsExpr", dsExpr), Map.entry("imbalanceExpr", imbalanceRenamedExpr));
    List<Component> components =
        dsExpr.getDataStructure().values().stream()
            .filter(Component::isIdentifier)
            .collect(Collectors.toList());
    DatasetExpression datasetExpression = executeLeftJoin(datasetExpressions, components);
    SparkDataset sparkDataset = asSparkDataset(datasetExpression);
    Dataset<Row> ds = sparkDataset.getSparkDataset();

    // TODO: Extract to a ValidationExpression(ResolvableExpression).
    Class errorCodeType = errorCodeExpr == null ? String.class : errorCodeExpr.getType();
    ResolvableExpression errorCodeExpression =
        ResolvableExpression.withType(errorCodeType)
            .withPosition(pos)
            .using(
                context -> {
                  Map<String, Object> contextMap = (Map<String, Object>) context;
                  if (errorCodeExpr == null) return null;
                  Object erCode = errorCodeExpr.resolve(contextMap);
                  Boolean boolVar = (Boolean) contextMap.get(BOOLVAR);
                  return boolVar ? null : errorCodeType.cast(erCode);
                });
    // TODO: Extract to a ValidationExpression(ResolvableExpression).
    Class errorLevelType = errorLevelExpr == null ? String.class : errorLevelExpr.getType();
    ResolvableExpression errorLevelExpression =
        ResolvableExpression.withType(errorLevelType)
            .withPosition(pos)
            .using(
                context -> {
                  Map<String, Object> contextMap = (Map<String, Object>) context;
                  if (errorLevelExpr == null) return null;
                  Object erLevel = errorLevelExpr.resolve(contextMap);
                  Boolean boolVar = (Boolean) contextMap.get(BOOLVAR);
                  return boolVar ? null : errorLevelType.cast(erLevel);
                });

    var roleMap = getRoleMap(sparkDataset);
    roleMap.put(ERRORLEVEL, MEASURE);
    roleMap.put(ERRORCODE, MEASURE);

    Map<String, ResolvableExpression> resolvableExpressions =
        Map.ofEntries(
            Map.entry(ERRORLEVEL, errorLevelExpression), Map.entry(ERRORCODE, errorCodeExpression));

    Dataset<Row> calculatedDataset = executeCalcEvaluated(ds, resolvableExpressions);
    DatasetExpression sparkDatasetExpression =
        new SparkDatasetExpression(new SparkDataset(calculatedDataset, roleMap), pos);

    // handle output: if none or all, return, if invalid filter on bool_var and return
    if (output == null || output.equals(ValidationOutput.ALL.value)) {
      return sparkDatasetExpression;
    }
    DatasetExpression filteredDataset =
        executeFilter(
            sparkDatasetExpression,
            ResolvableExpression.withType(Boolean.class).withPosition(pos).using(c -> null),
            BOOLVAR + " = false");
    // VTL issue: drop BOOLVAR in check_datapoint only specified but we apply also here for
    // harmonization
    Dataset<Row> result = asSparkDataset(filteredDataset).getSparkDataset().drop(BOOLVAR);
    return new SparkDatasetExpression(new SparkDataset(result), pos);
  }

  @Override
  public DatasetExpression executeHierarchicalValidation(
      DatasetExpression dsE,
      HierarchicalRuleset hr,
      String componentID,
      String validationMode,
      String inputMode,
      String validationOutput,
      Positioned pos) {
    // inputMode: dataset (default) | dataset_priority (not handled)
    if (inputMode != null && inputMode.equals("dataset_priority")) {
      throw new UnsupportedOperationException(
          "dataset_priority input mode is not supported in check_hierarchy");
    }

    // Create "bindings" (componentID column values)
    fr.insee.vtl.model.Dataset ds = dsE.resolve(Map.of());

    Map<String, Object> bindings =
        ds.getDataAsMap().stream()
            .collect(
                HashMap::new,
                (acc, dp) -> acc.put(dp.get(componentID).toString(), dp.get(hr.getVariable())),
                HashMap::putAll);
    // Save monomeasure type
    Component measure = dsE.getDataStructure().getMeasures().get(0);
    Class measureType = measure.getType();

    var roleMap = getRoleMap(ds);
    roleMap.put(RULEID, IDENTIFIER);
    roleMap.put(BOOLVAR, MEASURE);
    roleMap.put(IMBALANCE, MEASURE);
    roleMap.put(ERRORLEVEL, MEASURE);
    roleMap.put(ERRORCODE, MEASURE);

    Class errorCodeType = hr.getErrorCodeType();
    Class errorLevelType = hr.getErrorLevelType();

    List<DatasetExpression> datasetsExpression = new ArrayList<>();
    hr.getRules()
        .forEach(
            rule -> {
              DatasetExpression filteredDataset;
              try {
                filteredDataset =
                    executeFilterForHR(
                        dsE, componentID + " = \"" + rule.getValueDomainValue() + "\"");
              } catch (Exception e) {
                throw new RuntimeException(e);
              }

              String ruleName = rule.getName();
              List<String> codeItems = rule.getCodeItems();

              // handle validationMode
              Map<String, Object> ruleBindings = extractHRRuleBindings(bindings, codeItems);
              Boolean hasToProduceOutputLine = checkRule(codeItems, ruleBindings, validationMode);
              if (Boolean.FALSE.equals(hasToProduceOutputLine)) {
                // trick to break
                return;
              }
              ruleBindings =
                  buildBindingsWithDefault(ruleBindings, codeItems, validationMode, measureType);
              // Iterate on rules to resolve expressions
              Map<Object, Boolean> resolvedRuleExpressions = new HashMap<>();
              Map<Object, Double> resolvedLeftRuleExpressions = new HashMap<>();
              Map<Object, Double> resolvedRightRuleExpressions = new HashMap<>();
              // use try / catch because of scalar expr function resolution issue with null (only
              // handled in ds thanks to column type)
              try {
                resolvedRuleExpressions.put(
                    ruleName, (Boolean) rule.getExpression().resolve(ruleBindings));
              } catch (Exception e) {
                resolvedRuleExpressions.put(ruleName, null);
              }
              try {
                resolvedLeftRuleExpressions.put(
                    ruleName, (Double) rule.getLeftExpression().resolve(ruleBindings));
              } catch (Exception e) {
                resolvedLeftRuleExpressions.put(ruleName, null);
              }
              try {
                resolvedRightRuleExpressions.put(
                    ruleName, (Double) rule.getRightExpression().resolve(ruleBindings));
              } catch (Exception e) {
                resolvedRightRuleExpressions.put(ruleName, null);
              }
              ResolvableExpression ruleIdExpression =
                  ResolvableExpression.withType(String.class)
                      .withPosition(pos)
                      .using(context -> ruleName);

              String vd = rule.getValueDomainValue();
              ResolvableExpression valueDomainExpression =
                  ResolvableExpression.withType(String.class)
                      .withPosition(pos)
                      .using(context -> vd);

              Boolean expression = resolvedRuleExpressions.get(ruleName);

              ResolvableExpression errorCodeExpr = rule.getErrorCodeExpression();
              ResolvableExpression errorCodeExpression =
                  ResolvableExpression.withType(errorCodeType)
                      .withPosition(pos)
                      .using(
                          context -> {
                            if (errorCodeExpr == null || expression == null) return null;
                            Map<String, Object> mapContext = (Map<String, Object>) context;
                            Object erCode = errorCodeExpr.resolve(mapContext);
                            if (erCode == null) return null;
                            return expression.equals(Boolean.FALSE)
                                ? errorCodeType.cast(erCode)
                                : null;
                          });

              ResolvableExpression errorLevelExpr = rule.getErrorLevelExpression();
              ResolvableExpression errorLevelExpression =
                  ResolvableExpression.withType(errorLevelType)
                      .withPosition(pos)
                      .using(
                          context -> {
                            if (errorLevelExpr == null || expression == null) return null;
                            Map<String, Object> mapContext = (Map<String, Object>) context;
                            Object erLevel = errorLevelExpr.resolve(mapContext);
                            if (erLevel == null) return null;
                            return expression.equals(Boolean.FALSE)
                                ? errorLevelType.cast(erLevel)
                                : null;
                          });

              ResolvableExpression BoolvarExpression =
                  ResolvableExpression.withType(Boolean.class)
                      .withPosition(pos)
                      .using(context -> expression);

              ResolvableExpression imbalanceExpression =
                  ResolvableExpression.withType(measureType)
                      .withPosition(pos)
                      .using(
                          context -> {
                            Double leftExpression = resolvedLeftRuleExpressions.get(ruleName);
                            Double rightExpression = resolvedRightRuleExpressions.get(ruleName);
                            if (leftExpression == null || rightExpression == null) {
                              return null;
                            }
                            if (measureType.isAssignableFrom(Long.class)) {
                              return leftExpression.longValue() - rightExpression.longValue();
                            }
                            return leftExpression - rightExpression;
                          });

              Map<String, ResolvableExpression> resolvableExpressions = new HashMap<>();
              resolvableExpressions.put(RULEID, ruleIdExpression);
              resolvableExpressions.put(componentID, valueDomainExpression);
              resolvableExpressions.put(BOOLVAR, BoolvarExpression);
              resolvableExpressions.put(IMBALANCE, imbalanceExpression);
              resolvableExpressions.put(ERRORLEVEL, errorLevelExpression);
              resolvableExpressions.put(ERRORCODE, errorCodeExpression);

              datasetsExpression.add(
                  executeCalc(filteredDataset, resolvableExpressions, roleMap, Map.of()));
            });
    DatasetExpression datasetExpression;
    if (datasetsExpression.size() == 0) {
      InMemoryDataset emptyCHDataset =
          new InMemoryDataset(
              List.of(),
              Map.of(
                  measure.getName(),
                  measureType,
                  RULEID,
                  String.class,
                  componentID,
                  String.class,
                  BOOLVAR,
                  Boolean.class,
                  IMBALANCE,
                  Double.class,
                  ERRORLEVEL,
                  errorLevelType,
                  ERRORCODE,
                  errorCodeType),
              roleMap);
      datasetExpression = DatasetExpression.of(emptyCHDataset, pos);
    } else {
      datasetExpression = executeUnion(datasetsExpression);
    }
    // validationOutput invalid (default) | all | all_measures
    if (null == validationOutput || validationOutput.equals("invalid")) {
      DatasetExpression filteredDataset =
          executeFilter(
              datasetExpression,
              ResolvableExpression.withType(Boolean.class).withPosition(pos).using(c -> null),
              BOOLVAR + " = false");
      Dataset<Row> result = asSparkDataset(filteredDataset).getSparkDataset().drop(BOOLVAR);
      return new SparkDatasetExpression(new SparkDataset(result), pos);
    }
    if (validationOutput.equals("all")) {
      String measureName = measure.getName();
      Dataset<Row> result = asSparkDataset(datasetExpression).getSparkDataset().drop(measureName);
      return new SparkDatasetExpression(new SparkDataset(result), pos);
    }
    // all_measures
    return datasetExpression;
  }

  private DatasetExpression executeFilterForHR(DatasetExpression expression, String filterText)
      throws Exception {
    SparkDataset dataset = asSparkDataset(expression);
    Dataset<Row> ds = dataset.getSparkDataset();
    try {
      Dataset<Row> result = ds.filter(filterText);
      if (result.isEmpty()) {
        result = ds.limit(1);
      }
      return new SparkDatasetExpression(new SparkDataset(result, getRoleMap(dataset)), expression);
    } catch (Exception e) {
      throw new Exception(e);
    }
  }

  private Map<String, Object> extractHRRuleBindings(
      Map<String, Object> bindings, List<String> items) {
    Map<String, Object> ruleBindings = new HashMap<>();
    items.forEach(
        k -> {
          if (bindings.containsKey(k)) {
            Object value = bindings.get(k);
            ruleBindings.put(k, value);
          }
        });
    return ruleBindings;
  }

  private Boolean checkRule(
      List<String> codeItems, Map<String, Object> ruleBindings, String validationMode) {
    if (validationMode == null || validationMode.equals(NON_NULL)) {
      if (codeItems.size() != ruleBindings.size()) {
        return Boolean.FALSE;
      }
      if (ruleBindings.values().stream().noneMatch(Objects::isNull)) {
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    }
    if (validationMode.equals(NON_ZERO)) {
      if (ruleBindings.values().stream()
          .noneMatch(
              r -> {
                if (null == r) {
                  return Boolean.TRUE;
                }
                Double d = null;
                if (r.getClass().isAssignableFrom(Long.class)) {
                  d = ((Long) r).doubleValue();
                }
                if (r.getClass().isAssignableFrom(Double.class)) {
                  d = (Double) r;
                }
                if (d.equals(0D)) {
                  return Boolean.FALSE;
                }
                return Boolean.TRUE;
              })) {
        return Boolean.FALSE;
      }
      return Boolean.TRUE;
    }
    if (validationMode.equals(PARTIAL_NULL) || validationMode.equals(PARTIAL_ZERO)) {
      if (ruleBindings.values().stream().filter(Objects::nonNull).count() > 0) {
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    }
    if (validationMode.equals(ALWAYS_NULL) || validationMode.equals(ALWAYS_ZERO)) {
      return Boolean.TRUE;
    }
    return Boolean.FALSE;
  }

  private Map<String, Object> buildBindingsWithDefault(
      Map<String, Object> bindings,
      List<String> ruleItems,
      String validationMode,
      Class<?> measureType) {
    Map<String, Object> bindingsWithDefault = new HashMap<>();
    ruleItems.forEach(
        i -> {
          if (bindings.containsKey(i)) {
            bindingsWithDefault.put(i, bindings.get(i));
          } else {
            // don't need to handle non_null, items are always in bindings
            if (List.of(NON_ZERO, PARTIAL_ZERO, ALWAYS_ZERO).contains(validationMode)) {
              if (measureType.isAssignableFrom(Long.class)) {
                bindingsWithDefault.put(i, 0L);
              } else bindingsWithDefault.put(i, 0D);
            }
            if (List.of(PARTIAL_NULL, ALWAYS_NULL).contains(validationMode)) {
              bindingsWithDefault.put(i, null);
            }
          }
        });
    return bindingsWithDefault;
  }

  private <V, K> Map<V, K> invertMap(Map<K, V> map) {
    return map.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
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
   * The <code>Factory</code> class is an implementation of a VTL engine factory that returns Spark
   * engines.
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

package fr.insee.vtl.model;

import static fr.insee.vtl.model.Structured.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Interface used for dataset transformations. */
public interface ProcessingEngine {

  /**
   * Execute a calc transformations on the dataset expression.
   *
   * @param expression the dataset to apply the calc transformations on
   * @param expressions a map of expressions used to compute the new columns
   * @param roles a map of roles to apply to the new columns
   * @return the result of the calc transformation
   */
  DatasetExpression executeCalc(
      DatasetExpression expression,
      Map<String, ResolvableExpression> expressions,
      Map<String, Dataset.Role> roles,
      Map<String, String> expressionStrings);

  /**
   * Execute a filter transformations on the dataset expression.
   *
   * <p>
   *
   * @param expression the dataset to apply the filter transformations on
   * @param filter a filter expression
   * @return the result of the filter transformation
   */
  DatasetExpression executeFilter(
      DatasetExpression expression, ResolvableExpression filter, String filterString);

  /**
   * Execute a rename transformations on the dataset expression.
   *
   * @param expression the dataset to apply the rename transformations on
   * @param fromTo a map where key are the old name and values the new names
   * @return the result of the rename transformation
   */
  DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo);

  /**
   * Execute a project transformations on the dataset expression.
   *
   * @param expression the dataset to apply the project transformations on
   * @param columnNames a list of column names to keep
   * @return the result of the project transformation
   */
  DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames);

  /**
   * Execute a union transformations on the dataset expression.
   *
   * @param datasets list of dataset expression to union
   * @return the result of the union transformation
   */
  DatasetExpression executeUnion(List<DatasetExpression> datasets);

  /**
   * Execute an aggregate transformations on the dataset expression.
   *
   * <p>The API of this method is not stable yet.
   */
  DatasetExpression executeAggr(
      DatasetExpression expression,
      List<String> groupBy,
      Map<String, AggregationExpression> collectorMap);

  /**
   * Execute an simple analytic function (e.g. count, min, max) on the dataset expression based on a
   * given window specification (e.g. partitionBy, orderBy, datapoints)
   */
  DatasetExpression executeSimpleAnalytic(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      String sourceColumnName,
      List<String> partitionBy,
      Map<String, Analytics.Order> orderBy,
      Analytics.WindowSpec window);

  /**
   * Execute lead/lag analytic function on the dataset expression based on a given window
   * specification (e.g. partitionBy, orderBy).
   *
   * <p>Note lead and lag can't take a window frame (e.g. data points, range)
   */
  DatasetExpression executeLeadOrLagAn(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      String sourceColumnName,
      int offset,
      List<String> partitionBy,
      Map<String, Analytics.Order> orderBy);

  /**
   * Execute ratio_to_report analytic function on the dataset expression based on a given window
   * specification.
   *
   * <p>Note ratio_to_report can only take a partitionBy window specification
   */
  DatasetExpression executeRatioToReportAn(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      String sourceColumnName,
      List<String> partitionBy);

  /**
   * Execute rank analytic function on the dataset expression based on a given window specification.
   *
   * <p>Note rank can only take a window specification with partitionBy, orderBy. orderBy is
   * mandatory
   */
  DatasetExpression executeRankAn(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      List<String> partitionBy,
      Map<String, Analytics.Order> orderBy);

  /**
   * Execute a left join transformations on the dataset expressions.
   *
   * @param datasets a map of aliased datasets
   * @param components the components to join on
   * @return the result of the left join transformation
   */
  DatasetExpression executeLeftJoin(
      Map<String, DatasetExpression> datasets, List<Component> components);

  /**
   * Execute a inner join transformations on the dataset expressions.
   *
   * @param datasets a map of aliased datasets
   * @param components the components to join on
   * @return the result of the left join transformation
   */
  DatasetExpression executeInnerJoin(
      Map<String, DatasetExpression> datasets, List<Component> components);

  default DatasetExpression executeInnerJoin(Map<String, DatasetExpression> datasets) {
    Set<Component> commonIdentifiers =
        datasets.values().stream()
            .flatMap(datasetExpression -> datasetExpression.getDataStructure().values().stream())
            .filter(Structured.Component::isIdentifier)
            .collect(Collectors.toSet());
    return executeInnerJoin(datasets, new ArrayList<>(commonIdentifiers));
  }

  /**
   * Execute a cross join transformations on the dataset expressions.
   *
   * @param datasets a map of aliased datasets
   * @param identifiers the components to join on
   * @return the result of the left join transformation
   */
  DatasetExpression executeCrossJoin(
      Map<String, DatasetExpression> datasets, List<Component> identifiers);

  /**
   * Execute a full join transformations on the dataset expressions.
   *
   * @param datasets a map of aliased datasets
   * @param identifiers the components to join on
   * @return the result of the left join transformation
   */
  DatasetExpression executeFullJoin(
      Map<String, DatasetExpression> datasets, List<Component> identifiers);

  /**
   * Execute a validation DP ruleset on the dataset expressions.
   *
   * @param dpr datapoint ruleset
   * @param datasetExpression datasets
   * @param output validation output
   * @param pos script error position
   * @param toDrop variables to drop
   * @return the result of the validation DP ruleset transformation
   */
  DatasetExpression executeValidateDPruleset(
      DataPointRuleset dpr,
      DatasetExpression datasetExpression,
      String output,
      Positioned pos,
      List<String> toDrop);

  /**
   * Execute a simple validation on dataset expressions.
   *
   * @param dsExpr dataset expression
   * @param erCodeExpr error code expression
   * @param erLevelExpr error level expression
   * @param imbalanceExpr dataset expression
   * @param output validation output
   * @param pos script error position
   * @return the result of the validation
   */
  DatasetExpression executeValidationSimple(
      DatasetExpression dsExpr,
      ResolvableExpression erCodeExpr,
      ResolvableExpression erLevelExpr,
      DatasetExpression imbalanceExpr,
      String output,
      Positioned pos);

  ResolvableExpression executeHierarchicalValidation(
      DatasetExpression dsExpression,
      HierarchicalRuleset hr,
      String componentID,
      String validationMode,
      String inputMode,
      String validationOutput,
      Positioned pos);

  /**
   * Execute pivot on dataset expression.
   *
   * @param dsExpr dataset expression
   * @param idName identifier name
   * @param meName measure name
   * @param pos script error position
   * @return the result of the pivot
   */
  DatasetExpression executePivot(
      DatasetExpression dsExpr, String idName, String meName, Positioned pos);
}

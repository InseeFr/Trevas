package fr.insee.vtl.engine.semantics.aggregation;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.antlr.runtime.tree.ParseTree;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.Structured.DataStructure;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@code having} support for aggregate invocation and {@code [aggr …]}: copy source columns into
 * temporary fields, aggregate those fields in the same {@code group by}, filter, then drop temps.
 *
 * <p>Temporary copies match how {@code [aggr Me := sum(Me)]} normalizes aliases so Spark can
 * aggregate by output column name. {@code count()} reuses {@link AggregationNames#COUNT_MEASURE}.
 */
public final class HavingClauseApplier {

  private static final String TEMP_PREFIX = "__having_";

  private HavingClauseApplier() {}

  /**
   * @param existingCollectorNames collectors already planned for the result (so {@code count()} can
   *     reuse {@code int_var})
   */
  public static Plan plan(
      VtlParser.HavingClauseContext havingClause,
      DataStructure sourceStructure,
      Set<String> existingCollectorNames,
      Positioned position) {
    if (havingClause == null) {
      return Plan.EMPTY;
    }
    Map<String, AggregationExpression> extras = new LinkedHashMap<>();
    Map<String, String> tempSources = new LinkedHashMap<>();
    IdentityHashMap<VtlParser.AggrDatasetContext, String> tempByAggr = new IdentityHashMap<>();
    boolean needCountMeasure = false;
    int tempIndex = 0;

    for (ParseTree node : flatten(havingClause.expr())) {
      if (node instanceof VtlParser.CountAggrContext) {
        needCountMeasure = true;
        continue;
      }
      if (!(node instanceof VtlParser.AggrDatasetContext aggr)
          || aggr.groupingClause() != null
          || aggr.havingClause() != null) {
        continue;
      }
      if (aggr.COUNT() != null && aggr.expr() == null) {
        needCountMeasure = true;
        continue;
      }
      if (aggr.expr() == null) {
        continue;
      }
      String sourceColumn = VtlParseTrees.componentNameFromExpr(aggr.expr());
      Structured.Component component = sourceStructure.get(sourceColumn);
      if (component == null) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "having aggregate refers to unknown component " + sourceColumn, position));
      }
      String tempName = TEMP_PREFIX + tempIndex++;
      // Aggregate the temp column (filled by a prior calc copy of the source).
      extras.put(
          tempName,
          AggregationExpressionFactory.fromAggrDataset(
              aggr,
              AggregationColumnReferences.columnReference(
                  position, tempName, component.getType())));
      tempSources.put(tempName, sourceColumn);
      tempByAggr.put(aggr, tempName);
    }

    if (needCountMeasure
        && !existingCollectorNames.contains(AggregationNames.COUNT_MEASURE)
        && !extras.containsKey(AggregationNames.COUNT_MEASURE)) {
      extras.put(AggregationNames.COUNT_MEASURE, AggregationExpressionFactory.countRows());
    }
    return new Plan(extras, tempSources, tempByAggr);
  }

  /** Copies having source columns into temporary measure columns expected by {@link #plan}. */
  public static DatasetExpression materializeTemporaryColumns(
      DatasetExpression dataset, Plan plan, Positioned position, ProcessingEngine engine) {
    if (plan.temporarySources().isEmpty()) {
      return dataset;
    }
    Map<String, ResolvableExpression> expressions = new LinkedHashMap<>();
    Map<String, Dataset.Role> roles = new LinkedHashMap<>();
    Map<String, String> expressionStrings = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : plan.temporarySources().entrySet()) {
      String temp = entry.getKey();
      String source = entry.getValue();
      Structured.Component component = dataset.getDataStructure().get(source);
      expressions.put(
          temp, AggregationColumnReferences.columnReference(position, source, component.getType()));
      roles.put(temp, Dataset.Role.MEASURE);
      expressionStrings.put(temp, source);
    }
    return engine.executeCalc(dataset, expressions, roles, expressionStrings);
  }

  public static DatasetExpression apply(
      DatasetExpression aggregated,
      DataStructure finalStructure,
      VtlParser.HavingClauseContext havingClause,
      Plan plan,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine processingEngine) {
    if (havingClause == null) {
      return AggregationResults.withStructure(aggregated, finalStructure);
    }

    expressionVisitor.bindHavingAggregateColumns(plan.columnBindings(fromContext(havingClause)));
    try {
      ResolvableExpression filter = expressionVisitor.visit(havingClause.expr());
      DatasetExpression filtered =
          processingEngine.executeFilter(aggregated, filter, havingClause.getText());
      List<String> keep = new ArrayList<>(finalStructure.keySet());
      DatasetExpression projected = processingEngine.executeProject(filtered, keep);
      return AggregationResults.withStructure(projected, finalStructure);
    } finally {
      expressionVisitor.clearHavingAggregateColumns();
    }
  }

  private static List<ParseTree> flatten(ParseTree root) {
    List<ParseTree> nodes = new ArrayList<>();
    walk(root, nodes);
    return nodes;
  }

  private static void walk(ParseTree node, List<ParseTree> out) {
    out.add(node);
    for (int i = 0; i < node.getChildCount(); i++) {
      walk(node.getChild(i), out);
    }
  }

  public record Plan(
      Map<String, AggregationExpression> extraCollectors,
      Map<String, String> temporarySources,
      IdentityHashMap<VtlParser.AggrDatasetContext, String> temporaryColumnsByAggr) {

    static final Plan EMPTY = new Plan(Map.of(), Map.of(), new IdentityHashMap<>());

    IdentityHashMap<VtlParser.AggrDatasetContext, ResolvableExpression> columnBindings(
        Positioned position) {
      IdentityHashMap<VtlParser.AggrDatasetContext, ResolvableExpression> bindings =
          new IdentityHashMap<>();
      for (Map.Entry<VtlParser.AggrDatasetContext, String> entry :
          temporaryColumnsByAggr.entrySet()) {
        String name = entry.getValue();
        Class<?> type = extraCollectors.get(name).getType();
        bindings.put(
            entry.getKey(), AggregationColumnReferences.columnReference(position, name, type));
      }
      return bindings;
    }
  }
}

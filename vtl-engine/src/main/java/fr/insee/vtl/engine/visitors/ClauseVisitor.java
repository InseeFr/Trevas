package fr.insee.vtl.engine.visitors;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.assertBasicScalarType;
import static fr.insee.vtl.engine.utils.TypeChecking.assertNumber;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

/**
 * <code>ClauseVisitor</code> is the visitor for VTL clause expressions (component filter, aggr,
 * drop, etc.).
 */
public class ClauseVisitor extends VtlBaseVisitor<DatasetExpression> {

  private final DatasetExpression datasetExpression;
  private final ExpressionVisitor componentExpressionVisitor;

  private final ProcessingEngine processingEngine;

  /**
   * Constructor taking a dataset expression and a processing engine.
   *
   * @param datasetExpression The dataset expression containing the clause expression.
   * @param processingEngine The processing engine for dataset expressions.
   */
  public ClauseVisitor(
      DatasetExpression datasetExpression,
      ProcessingEngine processingEngine,
      VtlScriptEngine engine) {
    this.datasetExpression = Objects.requireNonNull(datasetExpression);
    // Here we "switch" to the dataset context.
    Map<String, Object> componentMap =
        datasetExpression.getDataStructure().values().stream()
            .collect(Collectors.toMap(Dataset.Component::getName, component -> component));
    this.componentExpressionVisitor = new ExpressionVisitor(componentMap, processingEngine, engine);
    this.processingEngine = Objects.requireNonNull(processingEngine);
  }

  public static String getName(VtlParser.ComponentIDContext context) {
    // TODO: Should be an expression so we can handle membership better
    //  and use the exceptions for undefined var etc.
    //        res := ds1[calc test := m1 * ds1#m2 + m3]
    //        res := ds1#m1 -> dataset with only m1.
    //        res := ceil(ds1#m1)
    //        res := ceil(ds1)
    String text = context.getText();
    if (text.startsWith("'") && text.endsWith("'")) {
      text = text.substring(1, text.length() - 1);
    }
    return text;
  }

  static String getSource(ParserRuleContext ctx) {
    var stream = ctx.getStart().getInputStream();
    return stream.getText(
        new Interval(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex()));
  }

  private static AggregationExpression convertToAggregation(
      VtlParser.AggrDatasetContext groupFunctionCtx, ResolvableExpression expression) {
    if (groupFunctionCtx.SUM() != null) {
      var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
      return AggregationExpression.sum(numberExpression);
    } else if (groupFunctionCtx.AVG() != null) {
      var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
      return AggregationExpression.avg(numberExpression);
    } else if (groupFunctionCtx.COUNT() != null) {
      return AggregationExpression.count();
    } else if (groupFunctionCtx.MAX() != null) {
      var typedExpression = assertBasicScalarType(expression, groupFunctionCtx.expr());
      return AggregationExpression.max(typedExpression);
    } else if (groupFunctionCtx.MIN() != null) {
      var typedExpression = assertBasicScalarType(expression, groupFunctionCtx.expr());
      return AggregationExpression.min(typedExpression);
    } else if (groupFunctionCtx.MEDIAN() != null) {
      var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
      return AggregationExpression.median(numberExpression);
    } else if (groupFunctionCtx.STDDEV_POP() != null) {
      var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
      return AggregationExpression.stdDevPop(numberExpression);
    } else if (groupFunctionCtx.STDDEV_SAMP() != null) {
      var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
      return AggregationExpression.stdDevSamp(numberExpression);
    } else if (groupFunctionCtx.VAR_POP() != null) {
      var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
      return AggregationExpression.varPop(numberExpression);
    } else if (groupFunctionCtx.VAR_SAMP() != null) {
      var numberExpression = assertNumber(expression, groupFunctionCtx.expr());
      return AggregationExpression.varSamp(numberExpression);
    } else {
      throw new VtlRuntimeException(
          new VtlScriptException("not implemented", fromContext(groupFunctionCtx)));
    }
  }

  @Override
  public DatasetExpression visitKeepOrDropClause(VtlParser.KeepOrDropClauseContext ctx) {
    // Normalize to keep operation.
    var keep = ctx.op.getType() == VtlParser.KEEP;
    var names = ctx.componentID().stream().map(ClauseVisitor::getName).collect(Collectors.toSet());
    List<String> columnNames =
        datasetExpression.getDataStructure().values().stream()
            .map(Dataset.Component::getName)
            .filter(name -> keep == names.contains(name))
            .collect(Collectors.toList());

    return processingEngine.executeProject(datasetExpression, columnNames);
  }

  @Override
  public DatasetExpression visitCalcClause(VtlParser.CalcClauseContext ctx) {

    var expressions = new LinkedHashMap<String, ResolvableExpression>();
    var expressionStrings = new LinkedHashMap<String, String>();
    var roles = new LinkedHashMap<String, Dataset.Role>();
    var currentDatasetExpression = datasetExpression;
    // TODO: Refactor so we call the executeCalc for each CalcClauseItemContext the same way we call
    // the
    //  analytics functions.
    for (VtlParser.CalcClauseItemContext calcCtx : ctx.calcClauseItem()) {
      var columnName = getName(calcCtx.componentID());
      var columnRole =
          calcCtx.componentRole() == null
              ? Dataset.Role.MEASURE
              : Dataset.Role.valueOf(calcCtx.componentRole().getText().toUpperCase());

      if ((calcCtx.expr() instanceof VtlParser.FunctionsExpressionContext)
          && ((VtlParser.FunctionsExpressionContext) calcCtx.expr()).functions()
              instanceof VtlParser.AnalyticFunctionsContext) {
        AnalyticsVisitor analyticsVisitor =
            new AnalyticsVisitor(processingEngine, currentDatasetExpression, columnName);
        VtlParser.FunctionsExpressionContext functionExprCtx =
            (VtlParser.FunctionsExpressionContext) calcCtx.expr();
        VtlParser.AnalyticFunctionsContext anFuncCtx =
            (VtlParser.AnalyticFunctionsContext) functionExprCtx.functions();
        currentDatasetExpression = analyticsVisitor.visit(anFuncCtx);
      } else {
        ResolvableExpression calc = componentExpressionVisitor.visit(calcCtx);

        expressions.put(columnName, calc);
        expressionStrings.put(columnName, getSource(calcCtx.expr()));
        roles.put(columnName, columnRole);
      }
    }

    if (!expressionStrings.isEmpty()) {
      currentDatasetExpression =
          processingEngine.executeCalc(
              currentDatasetExpression, expressions, roles, expressionStrings);
    }

    return currentDatasetExpression;
  }

  @Override
  public DatasetExpression visitFilterClause(VtlParser.FilterClauseContext ctx) {
    ResolvableExpression filter = componentExpressionVisitor.visit(ctx.expr());
    return processingEngine.executeFilter(datasetExpression, filter, getSource(ctx.expr()));
  }

  @Override
  public DatasetExpression visitRenameClause(VtlParser.RenameClauseContext ctx) {
    Map<String, String> fromTo = new LinkedHashMap<>();
    Set<String> renamed = new HashSet<>();
    for (VtlParser.RenameClauseItemContext renameCtx : ctx.renameClauseItem()) {
      var toNameString = getName(renameCtx.toName);
      var fromNameString = getName(renameCtx.fromName);
      if (!renamed.add(toNameString)) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "duplicate column: %s".formatted(toNameString), fromContext(renameCtx)));
      }
      fromTo.put(fromNameString, toNameString);
    }
    return processingEngine.executeRename(datasetExpression, fromTo);
  }

  @Override
  public DatasetExpression visitAggrClause(VtlParser.AggrClauseContext ctx) {

    // Normalize the dataset so the expressions are removed from the aggregations.
    var aggregationsWithExpressions =
        ctx.aggregateClause().aggrFunctionClause().stream()
            .filter(agg -> agg.aggrOperatorsGrouping() instanceof VtlParser.AggrDatasetContext)
            .collect(Collectors.toList());

    Map<String, ResolvableExpression> expressions =
        aggregationsWithExpressions.stream()
            .collect(
                Collectors.toMap(
                    agg -> getName(agg.componentID()),
                    agg ->
                        componentExpressionVisitor.visit(
                            ((VtlParser.AggrDatasetContext) agg.aggrOperatorsGrouping()).expr())));
    Map<String, Dataset.Role> roles =
        aggregationsWithExpressions.stream()
            .collect(
                Collectors.toMap(
                    agg -> getName(agg.componentID()),
                    agg ->
                        agg.componentRole() == null
                            ? Dataset.Role.MEASURE
                            : Dataset.Role.valueOf(agg.componentRole().getText().toUpperCase())));
    Map<String, String> expressionStrings =
        aggregationsWithExpressions.stream()
            .collect(
                Collectors.toMap(
                    agg -> getName(agg.componentID()),
                    agg -> getSource(agg.aggrOperatorsGrouping())));

    var dataStructure = datasetExpression.getDataStructure();

    DatasetExpression normalizedDataset =
        processingEngine.executeCalc(this.datasetExpression, expressions, roles, expressionStrings);

    // Execute the group all as a calc.
    List<String> groupBy = new ArrayList<>();
    var groupAll = new GroupAllVisitor(componentExpressionVisitor).visit(ctx);
    if (groupAll != null) {
      // TODO, use the name? What if the expression uses multiple columns.
      normalizedDataset =
          processingEngine.executeCalc(
              normalizedDataset,
              Map.of("time", groupAll),
              Map.of("time", Dataset.Role.IDENTIFIER),
              Map.of());
      groupBy.add("time");
    }

    // TODO: Move to engine
    Structured.DataStructure normalizedStructure = normalizedDataset.getDataStructure();

    // Transform column roles into IDENTIFIER when defined in groupingClause
    groupBy.addAll(new GroupByVisitor(dataStructure).visit(ctx));
    normalizedStructure.forEach(
        (k, v) -> {
          if (groupBy.contains(k)) {
            normalizedStructure.put(
                k, new Structured.Component(k, v.getType(), Dataset.Role.IDENTIFIER));
          }
        });

    Map<String, AggregationExpression> collectorMap = new LinkedHashMap<>();
    for (VtlParser.AggrFunctionClauseContext functionCtx :
        ctx.aggregateClause().aggrFunctionClause()) {
      String alias = getName(functionCtx.componentID());
      // TODO: Refactor to avoid this if
      if (normalizedStructure.containsKey(alias)) {
        Structured.Component normalizedComponent = normalizedStructure.get(alias);
        var aggregationFunction =
            convertToAggregation(
                // Note that here we replace the expression by the name of the columns.
                (VtlParser.AggrDatasetContext) functionCtx.aggrOperatorsGrouping(),
                new ResolvableExpression(fromContext(ctx)) {
                  @Override
                  public Object resolve(Map<String, Object> context) {
                    return context.get(alias);
                  }

                  @Override
                  public Class<?> getType() {
                    return normalizedComponent.getType();
                  }
                });
        collectorMap.put(alias, aggregationFunction);
      } else {
        collectorMap.put(alias, AggregationExpression.count());
      }
    }

    return processingEngine.executeAggr(normalizedDataset, groupBy, collectorMap);
  }

  @Override
  public DatasetExpression visitPivotOrUnpivotClause(VtlParser.PivotOrUnpivotClauseContext ctx) {
    if (ctx.op.equals(ctx.UNPIVOT())) {
      throw new UnsupportedOperationException("unpivot is not supported");
    }
    String id = ctx.id_.getText();
    if (!datasetExpression.getIdentifierNames().contains(id)) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              id + " is not part of the dataset identifiers", fromContext(ctx.id_)));
    }
    String me = ctx.mea.getText();
    if (!datasetExpression.getMeasureNames().contains(me)) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              me + " is not part of the dataset measures", fromContext(ctx.mea)));
    }
    Positioned pos = fromContext(ctx);
    return processingEngine.executePivot(datasetExpression, id, me, pos);
  }
}

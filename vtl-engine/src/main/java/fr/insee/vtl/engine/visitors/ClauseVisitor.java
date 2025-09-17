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

    // The type of the op can either be KEEP or DROP.
    final boolean keep = ctx.op.getType() == VtlParser.KEEP;

    // Columns explicitly requested in the KEEP/DROP clause
    final Set<String> requestedNames =
        ctx.componentID().stream()
            .map(ClauseVisitor::getName)
            .collect(Collectors.toCollection(LinkedHashSet::new)); // preserve user order

    // All available dataset components (ordered as in DataStructure)
    final List<Dataset.Component> componentsInOrder =
        new ArrayList<>(datasetExpression.getDataStructure().values());
    final List<String> allColumnsInOrder =
        componentsInOrder.stream().map(Dataset.Component::getName).collect(Collectors.toList());
    final Set<String> availableColumns = new LinkedHashSet<>(allColumnsInOrder);

    // Dataset identifiers (role = IDENTIFIER)
    final Map<String, Dataset.Component> identifiers =
        componentsInOrder.stream()
            .filter(c -> c.getRole() == Dataset.Role.IDENTIFIER)
            .collect(
                Collectors.toMap(
                    Dataset.Component::getName, c -> c, (a, b) -> a, LinkedHashMap::new));

    // Evaluate that all requested columns must exist in the dataset or raise an error
    for (String requested : requestedNames) {
      if (!availableColumns.contains(requested)) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                String.format(
                    "'%s' not found in dataset. Line %d, Statement: [%s]",
                    requested, ctx.getStart().getLine(), ctx.getText()),
                fromContext(ctx)));
      }
    }

    // VTL specification: identifiers must not appear explicitly in KEEP
    final Set<String> forbidden =
        requestedNames.stream()
            .filter(identifiers::containsKey)
            .collect(Collectors.toCollection(LinkedHashSet::new));

    if (!forbidden.isEmpty()) {
      StringBuilder details = new StringBuilder();
      for (String id : forbidden) {
        Dataset.Component comp = identifiers.get(id);
        details.append(
            String.format(
                "%s(role=%s, type=%s) ",
                id, comp.getRole(), comp.getType() != null ? comp.getType() : "n/a"));
      }
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              String.format(
                  "identifiers %s must not be explicitly listed in KEEP/DROP. Details: %s . Line %d, Statement: [%s]",
                  forbidden, details.toString().trim(), ctx.getStart().getLine(), ctx.getText()),
              fromContext(ctx)));
    }

    // Build result set:
    //  + KEEP: identifiers + requested columns
    //  + DROP: (all columns - requested) + identifiers
    final Set<String> resultSet = new LinkedHashSet<>();
    if (keep) {
      resultSet.addAll(identifiers.keySet());
      resultSet.addAll(requestedNames);
    } else {
      for (String col : allColumnsInOrder) {
        if (!requestedNames.contains(col)) {
          resultSet.add(col);
        }
      }
      // Ensure identifiers are always present
      resultSet.addAll(identifiers.keySet());
    }

    // Materialize result respecting dataset structure order
    final List<String> columnNames =
        allColumnsInOrder.stream().filter(resultSet::contains).collect(Collectors.toList());
    return processingEngine.executeProject(datasetExpression, columnNames);
  }

  @Override
  public DatasetExpression visitCalcClause(VtlParser.CalcClauseContext ctx) {

    // Dataset structure (ordered) and quick lookups
    final List<Dataset.Component> componentsInOrder =
        new ArrayList<>(datasetExpression.getDataStructure().values());

    final Map<String, Dataset.Component> byName =
        componentsInOrder.stream()
            .collect(
                Collectors.toMap(
                    Dataset.Component::getName, c -> c, (a, b) -> a, LinkedHashMap::new));

    // Accumulators for non-analytic calc items
    final LinkedHashMap<String, ResolvableExpression> expressions = new LinkedHashMap<>();
    final LinkedHashMap<String, String> expressionStrings = new LinkedHashMap<>();
    final LinkedHashMap<String, Dataset.Role> roles = new LinkedHashMap<>();

    // Tracks duplicates in the same clause (target names)
    final Set<String> targetsSeen = new LinkedHashSet<>();

    // We need a rolling dataset expression to chain analytics items
    DatasetExpression currentDatasetExpression = datasetExpression;

    // TODO: Refactor so we call executeCalc per CalcClauseItemContext (as analytics do).
    for (VtlParser.CalcClauseItemContext calcCtx : ctx.calcClauseItem()) {

      // ----  Resolve target name and desired role ----
      final String columnName = getName(calcCtx.componentID());
      final Dataset.Role columnRole =
          (calcCtx.componentRole() == null)
              ? Dataset.Role.MEASURE
              : Dataset.Role.valueOf(calcCtx.componentRole().getText().toUpperCase());

      // ---- Validate: duplicate target in the same clause ----
      if (!targetsSeen.add(columnName)) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                String.format(
                    "duplicate target '%s' in CALC clause. Line %d, Statement: [%s]",
                    columnName, ctx.getStart().getLine(), ctx.getText()),
                fromContext(ctx)));
      }

      // If the target already exists in the dataset, check its role
      final Dataset.Component existing = byName.get(columnName);
      if (existing != null) {
        // Explicitly block overwriting identifiers (already handled above if role==IDENTIFIER).
        if (existing.getRole() == Dataset.Role.IDENTIFIER) {
          final String meta =
              String.format(
                  "(role=%s, type=%s)",
                  existing.getRole(), existing.getType() != null ? existing.getType() : "n/a");
          throw new VtlRuntimeException(
              new InvalidArgumentException(
                  String.format(
                      "CALC cannot overwrite IDENTIFIER '%s' %s. Line %d, Statement: [%s]",
                      columnName, meta, ctx.getStart().getLine(), ctx.getText()),
                  fromContext(ctx)));
        }
      }

      // ---- Dispatch: analytics vs. regular calc ----
      final boolean isAnalytic =
          (calcCtx.expr() instanceof VtlParser.FunctionsExpressionContext)
              && ((VtlParser.FunctionsExpressionContext) calcCtx.expr()).functions()
                  instanceof VtlParser.AnalyticFunctionsContext;

      if (isAnalytic) {
        // Analytics are executed immediately and update the rolling dataset expression
        final AnalyticsVisitor analyticsVisitor =
            new AnalyticsVisitor(processingEngine, currentDatasetExpression, columnName);
        final VtlParser.FunctionsExpressionContext functionExprCtx =
            (VtlParser.FunctionsExpressionContext) calcCtx.expr();
        final VtlParser.AnalyticFunctionsContext anFuncCtx =
            (VtlParser.AnalyticFunctionsContext) functionExprCtx.functions();

        currentDatasetExpression = analyticsVisitor.visit(anFuncCtx);
      } else {
        // Regular calc expression – build resolvable expression and capture its source text
        final ResolvableExpression calc = componentExpressionVisitor.visit(calcCtx);

        final String exprSource = getSource(calcCtx.expr());
        if (exprSource == null || exprSource.isEmpty()) {
          throw new VtlRuntimeException(
              new InvalidArgumentException(
                  String.format(
                      "empty or unavailable source expression for '%s' in CALC. Line %d, Statement: [%s]",
                      columnName, ctx.getStart().getLine(), ctx.getText()),
                  fromContext(ctx)));
        }

        // Store in insertion order (deterministic column creation)
        expressions.put(columnName, calc);
        expressionStrings.put(columnName, exprSource);
        roles.put(columnName, columnRole);
      }
    }

    // ---- Consistency checks before execution ----
    if (!(expressions.keySet().equals(expressionStrings.keySet())
        && expressions.keySet().equals(roles.keySet()))) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              String.format(
                  "internal CALC maps out of sync (expressions/expressionStrings/roles). Line %d, Statement: [%s]",
                  ctx.getStart().getLine(), ctx.getText()),
              fromContext(ctx)));
    }

    // ---- Execute the batch calc if any non-analytic expressions were collected ----
    if (!expressionStrings.isEmpty()) {
      currentDatasetExpression =
          processingEngine.executeCalc(
              currentDatasetExpression, expressions, roles, expressionStrings);
    }
    return currentDatasetExpression;
  }

  @Override
  public DatasetExpression visitFilterClause(VtlParser.FilterClauseContext ctx) {

    // Error reporting context
    final int line = ctx.getStart().getLine();
    final int charPosition = ctx.getStart().getCharPositionInLine();
    final String statement = ctx.getText();

    ResolvableExpression filter = componentExpressionVisitor.visit(ctx.expr());
    return processingEngine.executeFilter(datasetExpression, filter, getSource(ctx.expr()));
  }

  @Override
  public DatasetExpression visitRenameClause(VtlParser.RenameClauseContext ctx) {

    // Dataset structure in order + lookup maps
    final List<Dataset.Component> componentsInOrder =
        new ArrayList<>(datasetExpression.getDataStructure().values());
    final Set<String> availableColumns =
        componentsInOrder.stream()
            .map(Dataset.Component::getName)
            .collect(Collectors.toCollection(LinkedHashSet::new));

    // Map for detailed error reporting (includes role/type if available)
    final Map<String, Dataset.Component> byName =
        componentsInOrder.stream()
            .collect(
                Collectors.toMap(
                    Dataset.Component::getName, c -> c, (a, b) -> a, LinkedHashMap::new));

    // Parse the RENAME clause and validate
    Map<String, String> fromTo = new LinkedHashMap<>();
    Set<String> toSeen = new LinkedHashSet<>();
    Set<String> fromSeen = new LinkedHashSet<>();

    for (VtlParser.RenameClauseItemContext renameCtx : ctx.renameClauseItem()) {
      final String toNameString = getName(renameCtx.toName);
      final String fromNameString = getName(renameCtx.fromName);

      // Validate: no duplicate "from" names inside the clause
      if (!fromSeen.add(fromNameString)) {
        String errorMsg =
            String.format(
                "Error: duplicate source name in RENAME clause: '%s. Line %d, Statement: [%s]",
                fromNameString, ctx.getStart().getLine(), ctx.getText());
        throw new VtlRuntimeException(new InvalidArgumentException(errorMsg, fromContext(ctx)));
      }

      // Validate: "from" must exist in dataset
      if (!availableColumns.contains(fromNameString)) {
        Dataset.Component comp = byName.get(fromNameString);
        String meta =
            (comp != null)
                ? String.format(
                    " (role=%s, type=%s)",
                    comp.getRole(), comp.getType() != null ? comp.getType() : "n/a")
                : "";
        String errorMsg =
            String.format(
                "Error: source column to rename not found: '%s'%s. Line %d, Statement: [%s]",
                fromNameString, meta, ctx.getStart().getLine(), ctx.getText());
        throw new VtlRuntimeException(new InvalidArgumentException(errorMsg, fromContext(ctx)));
      }

      // Validate: no duplicate "to" names inside the clause
      if (!toSeen.add(toNameString)) {
        String errorMsg =
            String.format(
                "Error: duplicate output column name in RENAME clause: '%s. Line %d, Statement: [%s]",
                fromNameString, ctx.getStart().getLine(), ctx.getText());
        throw new VtlRuntimeException(new InvalidArgumentException(errorMsg, fromContext(ctx)));
      }

      fromTo.put(fromNameString, toNameString);
    }

    // Validate collisions with untouched dataset columns ("Untouched" = columns that are not
    // being renamed)
    final Set<String> untouched =
        availableColumns.stream()
            .filter(c -> !fromTo.containsKey(c))
            .collect(Collectors.toCollection(LinkedHashSet::new));

    for (Map.Entry<String, String> e : fromTo.entrySet()) {
      final String from = e.getKey();
      final String to = e.getValue();

      // If target already exists as untouched, it would cause a collision
      if (untouched.contains(to)) {
        Dataset.Component comp = byName.get(to);
        String meta =
            (comp != null)
                ? String.format(
                    " (role=%s, type=%s)",
                    comp.getRole(), comp.getType() != null ? comp.getType() : "n/a")
                : "";
        String errorMsg =
            String.format(
                "Error: target name '%s'%s already exists in dataset and is not being renamed. Line %d, Statement: [%s]",
                to, meta, ctx.getStart().getLine(), ctx.getText());
        throw new VtlRuntimeException(new InvalidArgumentException(errorMsg, fromContext(ctx)));
      }
    }

    // Execute rename in processing engine
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

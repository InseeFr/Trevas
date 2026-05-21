package fr.insee.vtl.engine.visitors;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.VtlScriptEngine.toPositioned;

import fr.insee.vtl.antlr.runtime.ParserRuleContext;
import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.aggregation.AggrClauseExecutor;
import fr.insee.vtl.engine.aggregation.VtlParseTrees;
import fr.insee.vtl.engine.attribute.ComponentRoles;
import fr.insee.vtl.engine.exceptions.AlreadyDefinedException;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    return VtlParseTrees.componentName(context);
  }

  static String getSource(ParserRuleContext ctx) {
    return VtlParseTrees.sourceText(ctx);
  }

  @Override
  public DatasetExpression visitKeepOrDropClause(VtlParser.KeepOrDropClauseContext ctx) {

    // The type of the op can either be KEEP or DROP.
    boolean keep = ctx.op.getType() == VtlParser.KEEP;

    // Dataset identifiers (role = IDENTIFIER)
    Map<String, Dataset.Component> identifiers =
        datasetExpression.getDataStructure().getIdentifiers().stream()
            .collect(Collectors.toMap(Structured.Component::getName, Function.identity()));

    var columns =
        ctx.componentID().stream()
            .collect(Collectors.toMap(ClauseVisitor::getName, Function.identity()));

    var structure = datasetExpression.getDataStructure();

    // Evaluate that all requested columns must exist in the dataset or raise an error
    for (String col : columns.keySet()) {
      if (!structure.containsKey(col)) {
        throw new VtlRuntimeException(
            new UndefinedVariableException(col, fromContext(columns.get(col))));
      }
    }

    // VTL specification: identifiers must not appear explicitly in KEEP
    for (String col : columns.keySet()) {
      if (structure.get(col).isIdentifier()) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "cannot keep/drop identifiers", fromContext(columns.get(col))));
      }
    }

    // Build result set:
    //  + KEEP: identifiers + requested columns
    //  + DROP: (all columns - requested) + identifiers
    final Set<String> resultSet = new LinkedHashSet<>();
    resultSet.addAll(identifiers.keySet());
    if (keep) {
      resultSet.addAll(columns.keySet());
    } else {
      for (String col : structure.keySet()) {
        if (!columns.keySet().contains(col)) {
          resultSet.add(col);
        }
      }
    }

    // Retrieve the output column names (identifiers + requested)
    final List<String> outputColumns =
        structure.keySet().stream().filter(resultSet::contains).collect(Collectors.toList());
    return processingEngine.executeProject(datasetExpression, outputColumns);
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
              : ComponentRoles.fromParser(calcCtx.componentRole());

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
                  // TODO: see if other cases are the same error (already defined in assignment for
                  // example).
                  String.format("CALC cannot overwrite IDENTIFIER '%s' %s.", columnName, meta),
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
                      "empty or unavailable source expression for '%s' in CALC.", columnName),
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
              "internal CALC maps out of sync (expressions/expressionStrings/roles)",
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
    ResolvableExpression filter = componentExpressionVisitor.visit(ctx.expr());
    return processingEngine.executeFilter(datasetExpression, filter, getSource(ctx.expr()));
  }

  @Override
  public DatasetExpression visitRenameClause(VtlParser.RenameClauseContext ctx) {

    // Dataset structure in order + lookup maps
    // final List<Dataset.Component> componentsInOrder =
    //    new ArrayList<>(datasetExpression.getDataStructure().values());
    // final Set<String> availableColumns =
    //    componentsInOrder.stream()
    //        .map(Dataset.Component::getName)
    //        .collect(Collectors.toCollection(LinkedHashSet::new));

    var structure = datasetExpression.getDataStructure();

    // Parse the RENAME clause and validate
    Map<String, String> fromTo = new LinkedHashMap<>();
    Set<String> toSeen = new LinkedHashSet<>();
    Set<String> fromSeen = new LinkedHashSet<>();

    Map<String, ParserRuleContext> toCtxMap = new HashMap<>();
    Map<String, ParserRuleContext> fromCtxMap = new HashMap<>();

    for (VtlParser.RenameClauseItemContext renameCtx : ctx.renameClauseItem()) {
      toCtxMap.put(getName(renameCtx.toName), renameCtx.toName);
      fromCtxMap.put(getName(renameCtx.fromName), renameCtx.fromName);

      final String toNameString = getName(renameCtx.toName);
      final String fromNameString = getName(renameCtx.fromName);

      // Validate: no duplicate "from" names inside the clause
      if (!fromSeen.add(fromNameString)) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "duplicate from name '%s'".formatted(renameCtx.fromName.getText()),
                toPositioned(renameCtx.fromName)));
      }

      // Validate: "from" must exist in dataset
      if (!structure.containsKey(fromNameString)) {
        throw new VtlRuntimeException(
            new UndefinedVariableException(toPositioned(renameCtx.fromName), datasetExpression));
      }

      // Validate: no duplicate "to" names inside the clause
      if (!toSeen.add(toNameString)) {
        throw new VtlRuntimeException(
            new AlreadyDefinedException(toPositioned(renameCtx.toName), datasetExpression));
      }

      fromTo.put(fromNameString, toNameString);
    }

    // Check that the renamed columns do not collide with the remaining columns. This
    // is done so that swapping variables works: a -> b, b -> a.
    final Set<String> untouched =
        structure.keySet().stream()
            .filter(c -> !fromTo.containsKey(c))
            .collect(Collectors.toCollection(LinkedHashSet::new));

    for (Map.Entry<String, String> e : fromTo.entrySet()) {
      final String from = e.getKey();
      final String to = e.getValue();

      // If target already exists as untouched, it would cause a collision
      if (untouched.contains(to)) {
        throw new VtlRuntimeException(
            new AlreadyDefinedException(toPositioned(toCtxMap.get(to)), datasetExpression));
      }
    }

    // Execute rename in processing engine
    return processingEngine.executeRename(datasetExpression, fromTo);
  }

  @Override
  public DatasetExpression visitAggrClause(VtlParser.AggrClauseContext ctx) {
    return AggrClauseExecutor.execute(
        datasetExpression, ctx, componentExpressionVisitor, processingEngine);
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

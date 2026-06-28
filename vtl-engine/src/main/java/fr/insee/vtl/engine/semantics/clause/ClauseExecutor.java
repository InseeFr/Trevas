package fr.insee.vtl.engine.semantics.clause;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.VtlScriptEngine.toPositioned;

import fr.insee.vtl.antlr.runtime.ParserRuleContext;
import fr.insee.vtl.engine.exceptions.AlreadyDefinedException;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.semantics.aggregation.AggrClauseExecutor;
import fr.insee.vtl.engine.semantics.aggregation.VtlParseTrees;
import fr.insee.vtl.engine.semantics.analytic.AnalyticExecutor;
import fr.insee.vtl.engine.semantics.attribute.ComponentRoles;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** VTL dataset-clause orchestration on top of {@link ProcessingEngine}. */
public final class ClauseExecutor {

  private ClauseExecutor() {}

  public static DatasetExpression keepOrDrop(
      DatasetExpression dataset, VtlParser.KeepOrDropClauseContext ctx, ProcessingEngine engine) {
    boolean keep = ctx.op.getType() == VtlParser.KEEP;

    Map<String, Dataset.Component> identifiers =
        dataset.getDataStructure().getIdentifiers().stream()
            .collect(Collectors.toMap(Structured.Component::getName, Function.identity()));

    var columns =
        ctx.componentID().stream()
            .collect(Collectors.toMap(c -> VtlParseTrees.componentName(c), Function.identity()));

    var structure = dataset.getDataStructure();

    for (String col : columns.keySet()) {
      if (!structure.containsKey(col)) {
        throw new VtlRuntimeException(
            new UndefinedVariableException(col, fromContext(columns.get(col))));
      }
    }

    for (String col : columns.keySet()) {
      if (structure.get(col).isIdentifier()) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "cannot keep/drop identifiers", fromContext(columns.get(col))));
      }
    }

    final Set<String> resultSet = new LinkedHashSet<>();
    resultSet.addAll(identifiers.keySet());
    if (keep) {
      resultSet.addAll(columns.keySet());
    } else {
      for (String col : structure.keySet()) {
        if (!columns.containsKey(col)) {
          resultSet.add(col);
        }
      }
    }

    List<String> outputColumns =
        structure.keySet().stream().filter(resultSet::contains).collect(Collectors.toList());
    return engine.executeProject(dataset, outputColumns);
  }

  public static DatasetExpression calc(
      DatasetExpression dataset,
      VtlParser.CalcClauseContext ctx,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine engine) {

    final List<Dataset.Component> componentsInOrder =
        new ArrayList<>(dataset.getDataStructure().values());

    final Map<String, Dataset.Component> byName =
        componentsInOrder.stream()
            .collect(
                Collectors.toMap(
                    Dataset.Component::getName, c -> c, (a, b) -> a, LinkedHashMap::new));

    final LinkedHashMap<String, ResolvableExpression> expressions = new LinkedHashMap<>();
    final LinkedHashMap<String, String> expressionStrings = new LinkedHashMap<>();
    final LinkedHashMap<String, Dataset.Role> roles = new LinkedHashMap<>();

    DatasetExpression current = dataset;

    for (VtlParser.CalcClauseItemContext calcCtx : ctx.calcClauseItem()) {
      final String columnName = VtlParseTrees.componentName(calcCtx.componentID());
      final Dataset.Role columnRole =
          (calcCtx.componentRole() == null)
              ? Dataset.Role.MEASURE
              : ComponentRoles.fromParser(calcCtx.componentRole());

      final Dataset.Component existing = byName.get(columnName);
      if (existing != null && existing.getRole() == Dataset.Role.IDENTIFIER) {
        final String meta =
            String.format(
                "(role=%s, type=%s)",
                existing.getRole(), existing.getType() != null ? existing.getType() : "n/a");
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                String.format("CALC cannot overwrite IDENTIFIER '%s' %s.", columnName, meta),
                fromContext(ctx)));
      }

      final boolean isAnalytic =
          (calcCtx.expr() instanceof VtlParser.FunctionsExpressionContext)
              && ((VtlParser.FunctionsExpressionContext) calcCtx.expr()).functions()
                  instanceof VtlParser.AnalyticFunctionsContext;

      if (isAnalytic) {
        final VtlParser.FunctionsExpressionContext functionExprCtx =
            (VtlParser.FunctionsExpressionContext) calcCtx.expr();
        final VtlParser.AnalyticFunctionsContext anFuncCtx =
            (VtlParser.AnalyticFunctionsContext) functionExprCtx.functions();

        current = AnalyticExecutor.execute(anFuncCtx, engine, current, columnName);
      } else {
        final ResolvableExpression calc = expressionVisitor.visit(calcCtx);
        final String exprSource = VtlParseTrees.sourceText(calcCtx.expr());
        if (exprSource == null || exprSource.isEmpty()) {
          throw new VtlRuntimeException(
              new InvalidArgumentException(
                  String.format(
                      "empty or unavailable source expression for '%s' in CALC.", columnName),
                  fromContext(ctx)));
        }
        expressions.put(columnName, calc);
        expressionStrings.put(columnName, exprSource);
        roles.put(columnName, columnRole);
      }
    }

    if (!(expressions.keySet().equals(expressionStrings.keySet())
        && expressions.keySet().equals(roles.keySet()))) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "internal CALC maps out of sync (expressions/expressionStrings/roles)",
              fromContext(ctx)));
    }

    if (!expressionStrings.isEmpty()) {
      current = engine.executeCalc(current, expressions, roles, expressionStrings);
    }
    return current;
  }

  public static DatasetExpression filter(
      DatasetExpression dataset,
      VtlParser.FilterClauseContext ctx,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine engine) {
    ResolvableExpression filter = expressionVisitor.visit(ctx.expr());
    return engine.executeFilter(dataset, filter, VtlParseTrees.sourceText(ctx.expr()));
  }

  public static DatasetExpression rename(
      DatasetExpression dataset, VtlParser.RenameClauseContext ctx, ProcessingEngine engine) {
    var structure = dataset.getDataStructure();
    Map<String, String> fromTo = new LinkedHashMap<>();
    Set<String> toSeen = new LinkedHashSet<>();
    Set<String> fromSeen = new LinkedHashSet<>();
    Map<String, ParserRuleContext> toCtxMap = new HashMap<>();
    Map<String, ParserRuleContext> fromCtxMap = new HashMap<>();

    for (VtlParser.RenameClauseItemContext renameCtx : ctx.renameClauseItem()) {
      String toName = VtlParseTrees.componentName(renameCtx.toName);
      String fromName = VtlParseTrees.componentName(renameCtx.fromName);
      toCtxMap.put(toName, renameCtx.toName);
      fromCtxMap.put(fromName, renameCtx.fromName);

      if (!fromSeen.add(fromName)) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "duplicate from name '%s'".formatted(renameCtx.fromName.getText()),
                toPositioned(renameCtx.fromName)));
      }
      if (!structure.containsKey(fromName)) {
        throw new VtlRuntimeException(
            new UndefinedVariableException(toPositioned(renameCtx.fromName), dataset));
      }
      if (!toSeen.add(toName)) {
        throw new VtlRuntimeException(
            new AlreadyDefinedException(toPositioned(renameCtx.toName), dataset));
      }
      fromTo.put(fromName, toName);
    }

    final Set<String> untouched =
        structure.keySet().stream()
            .filter(c -> !fromTo.containsKey(c))
            .collect(Collectors.toCollection(LinkedHashSet::new));

    for (Map.Entry<String, String> e : fromTo.entrySet()) {
      if (untouched.contains(e.getValue())) {
        throw new VtlRuntimeException(
            new AlreadyDefinedException(toPositioned(toCtxMap.get(e.getValue())), dataset));
      }
    }

    return engine.executeRename(dataset, fromTo);
  }

  public static DatasetExpression subspace(
      DatasetExpression dataset,
      VtlParser.SubspaceClauseContext ctx,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine engine) {
    return SubspaceClauseExecutor.execute(dataset, ctx, expressionVisitor, engine);
  }

  public static DatasetExpression aggr(
      DatasetExpression dataset,
      VtlParser.AggrClauseContext ctx,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine engine) {
    return AggrClauseExecutor.execute(dataset, ctx, expressionVisitor, engine);
  }

  public static DatasetExpression pivot(
      DatasetExpression dataset,
      VtlParser.PivotOrUnpivotClauseContext ctx,
      ProcessingEngine engine) {
    if (ctx.op.equals(ctx.UNPIVOT())) {
      throw new UnsupportedOperationException("unpivot is not supported");
    }
    String id = ctx.id_.getText();
    if (!dataset.getIdentifierNames().contains(id)) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              id + " is not part of the dataset identifiers", fromContext(ctx.id_)));
    }
    String me = ctx.mea.getText();
    if (!dataset.getMeasureNames().contains(me)) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              me + " is not part of the dataset measures", fromContext(ctx.mea)));
    }
    return engine.executePivot(dataset, id, me, fromContext(ctx));
  }
}

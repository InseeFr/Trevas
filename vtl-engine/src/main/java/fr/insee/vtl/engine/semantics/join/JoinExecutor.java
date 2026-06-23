package fr.insee.vtl.engine.semantics.join;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;

import fr.insee.vtl.antlr.runtime.ParserRuleContext;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.semantics.DatasetResults;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** VTL join orchestration on top of mechanical {@link ProcessingEngine} join primitives. */
public final class JoinExecutor {

  private static final String MUST_HAVE_COMMON_IDENTIFIERS =
      "datasets must have common identifiers";

  private JoinExecutor() {}

  public static DatasetExpression leftJoin(
      VtlParser.JoinExprContext ctx, ExpressionVisitor expressionVisitor, ProcessingEngine engine) {
    var joinClause = ctx.joinClause();
    var operands = normalizeOperands(joinClause.joinClauseItem(), expressionVisitor);
    List<Component> keys = resolveJoinKeys(joinClause, operands);
    return finalizeJoin(
        engine,
        mechanicalLeftJoin(engine, renameDuplicates(keys, operands, engine), keys),
        keys,
        operands);
  }

  public static DatasetExpression innerJoin(
      VtlParser.JoinExprContext ctx, ExpressionVisitor expressionVisitor, ProcessingEngine engine) {
    var joinClause = ctx.joinClause();
    var operands = normalizeOperands(joinClause.joinClauseItem(), expressionVisitor);
    List<Component> keys = resolveJoinKeys(joinClause, operands);
    return finalizeJoin(
        engine,
        mechanicalInnerJoin(engine, renameDuplicates(keys, operands, engine), keys),
        keys,
        operands);
  }

  public static DatasetExpression fullJoin(
      VtlParser.JoinExprContext ctx, ExpressionVisitor expressionVisitor, ProcessingEngine engine) {
    var joinClause = ctx.joinClauseWithoutUsing();
    var operands = normalizeOperands(joinClause.joinClauseItem(), expressionVisitor);
    List<Component> keys = requireCommonIdentifiers(operands.values(), joinClause);
    return finalizeJoin(
        engine,
        mechanicalFullJoin(engine, renameDuplicates(keys, operands, engine), keys),
        keys,
        operands);
  }

  public static DatasetExpression crossJoin(
      VtlParser.JoinExprContext ctx, ExpressionVisitor expressionVisitor, ProcessingEngine engine) {
    var joinClause = ctx.joinClauseWithoutUsing();
    var operands = normalizeOperands(joinClause.joinClauseItem(), expressionVisitor);
    Map<String, DatasetExpression> renamed = renameDuplicates(List.of(), operands, engine);
    DatasetExpression joined = mechanicalCrossJoin(engine, renamed);
    if (JoinResultColumnOrder.hasAliasedColumn(joined.getDataStructure())) {
      List<DataStructure> structures =
          renamed.values().stream().map(DatasetExpression::getDataStructure).toList();
      if (structures.size() == 2) {
        return JoinFinalization.apply(
            engine, joined, JoinResultColumnOrder.crossJoinTwoOperandColumnOrder(structures));
      }
      return joined;
    }
    return finalizeJoin(engine, joined, List.of(), operands);
  }

  public static DatasetExpression innerJoinInferringKeys(
      ProcessingEngine engine, Map<String, DatasetExpression> datasets) {
    List<Component> keys = inferredJoinKeys(datasets);
    return mechanicalInnerJoin(engine, datasets, keys);
  }

  private static DatasetExpression mechanicalInnerJoin(
      ProcessingEngine engine, Map<String, DatasetExpression> datasets, List<Component> keys) {
    return withJoinStructure(engine.executeInnerJoin(datasets, keys), keys, datasets.values());
  }

  private static DatasetExpression mechanicalLeftJoin(
      ProcessingEngine engine, Map<String, DatasetExpression> datasets, List<Component> keys) {
    return withJoinStructure(engine.executeLeftJoin(datasets, keys), keys, datasets.values());
  }

  private static DatasetExpression mechanicalFullJoin(
      ProcessingEngine engine, Map<String, DatasetExpression> datasets, List<Component> keys) {
    return withJoinStructure(engine.executeFullJoin(datasets, keys), keys, datasets.values());
  }

  private static DatasetExpression mechanicalCrossJoin(
      ProcessingEngine engine, Map<String, DatasetExpression> datasets) {
    return withJoinStructure(
        engine.executeCrossJoin(datasets, List.of()), List.of(), datasets.values());
  }

  private static DatasetExpression finalizeJoin(
      ProcessingEngine engine,
      DatasetExpression joined,
      List<Component> keys,
      LinkedHashMap<String, DatasetExpression> operands) {
    List<DataStructure> operandStructures =
        operands.values().stream().map(DatasetExpression::getDataStructure).toList();
    List<String> columnOrder =
        JoinResultColumnOrder.compute(joined.getDataStructure(), keys, operandStructures);
    return JoinFinalization.apply(engine, joined, columnOrder);
  }

  private static List<Component> inferredJoinKeys(Map<String, DatasetExpression> datasets) {
    return datasets.values().stream()
        .flatMap(dataset -> dataset.getDataStructure().values().stream())
        .filter(Component::isIdentifier)
        .collect(
            Collectors.collectingAndThen(
                Collectors.toCollection(LinkedHashSet::new), ArrayList::new));
  }

  private static DatasetExpression withJoinStructure(
      DatasetExpression mechanical, List<Component> keys, Collection<DatasetExpression> operands) {
    List<DataStructure> structures =
        operands.stream().map(DatasetExpression::getDataStructure).toList();
    return DatasetResults.withStructure(mechanical, JoinStructureBuilder.build(keys, structures));
  }

  private static LinkedHashMap<String, DatasetExpression> normalizeOperands(
      List<VtlParser.JoinClauseItemContext> items, ExpressionVisitor expressionVisitor) {
    LinkedHashMap<String, DatasetExpression> operands = new LinkedHashMap<>();
    List<String> measures = new ArrayList<>();
    for (VtlParser.JoinClauseItemContext item : items) {
      var exprCtx = item.expr();
      String alias = item.alias() != null ? item.alias().IDENTIFIER().getText() : null;
      if (alias == null && !(exprCtx instanceof VtlParser.VarIdExprContext)) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "cannot use expression without alias in join clause", fromContext(exprCtx)));
      }
      DatasetExpression dataset =
          (DatasetExpression)
              assertTypeExpression(expressionVisitor.visit(exprCtx), Dataset.class, exprCtx);
      List<String> dsMeasures =
          dataset.getDataStructure().values().stream()
              .filter(Component::isMeasure)
              .map(Component::getName)
              .toList();
      if (alias == null) {
        dsMeasures.forEach(
            m -> {
              if (measures.contains(m)) {
                throw new VtlRuntimeException(
                    new InvalidArgumentException(
                        "It is not allowed that two or more Components in the virtual Data Set have the same name ("
                            + m
                            + ")",
                        fromContext(exprCtx)));
              }
            });
        operands.put(exprCtx.getText(), dataset);
      } else {
        operands.put(alias, dataset);
      }
      measures.addAll(dsMeasures);
    }
    return operands;
  }

  private static Map<String, DatasetExpression> renameDuplicates(
      List<Component> identifiers,
      Map<String, DatasetExpression> operands,
      ProcessingEngine engine) {
    Set<String> identifierNames =
        identifiers.stream().map(Component::getName).collect(Collectors.toSet());
    Set<String> duplicates = new LinkedHashSet<>();
    Set<String> uniques = new LinkedHashSet<>();
    for (DatasetExpression dataset : operands.values()) {
      for (String name : dataset.getColumnNames()) {
        if (identifierNames.contains(name)) {
          continue;
        }
        if (!uniques.add(name)) {
          duplicates.add(name);
        }
      }
    }
    Map<String, DatasetExpression> result = new LinkedHashMap<>();
    for (Map.Entry<String, DatasetExpression> entry : operands.entrySet()) {
      String alias = entry.getKey();
      DatasetExpression dataset = entry.getValue();
      Map<String, String> fromTo = new LinkedHashMap<>();
      for (String columnName : dataset.getColumnNames()) {
        if (duplicates.contains(columnName)) {
          fromTo.put(columnName, alias + "#" + columnName);
        }
      }
      result.put(alias, fromTo.isEmpty() ? dataset : engine.executeRename(dataset, fromTo));
    }
    return result;
  }

  private static List<Component> resolveJoinKeys(
      VtlParser.JoinClauseContext joinClause, LinkedHashMap<String, DatasetExpression> operands) {
    if (joinClause.USING() == null) {
      return requireCommonIdentifiers(operands.values(), joinClause);
    }
    List<Component> keys = new ArrayList<>();
    for (VtlParser.ComponentIDContext usingContext : joinClause.componentID()) {
      String name = usingContext.getText();
      for (DatasetExpression dataset : operands.values()) {
        if (!dataset.getColumnNames().contains(name)) {
          throw new VtlRuntimeException(
              new InvalidArgumentException(
                  "using component " + name + " is not present in all datasets",
                  fromContext(usingContext)));
        }
        if (!dataset.getDataStructure().get(name).isIdentifier()) {
          throw new VtlRuntimeException(
              new InvalidArgumentException(
                  "using component " + name + " has to be an identifier",
                  fromContext(usingContext)));
        }
      }
      Component component =
          operands.values().iterator().next().getDataStructure().values().stream()
              .filter(c -> c.getName().equals(name))
              .toList()
              .get(0);
      keys.add(component);
    }
    return keys;
  }

  private static List<Component> requireCommonIdentifiers(
      Collection<DatasetExpression> datasets, ParserRuleContext ctx) {
    return sameIdentifiers(datasets)
        .orElseThrow(
            () ->
                new VtlRuntimeException(
                    new InvalidArgumentException(MUST_HAVE_COMMON_IDENTIFIERS, fromContext(ctx))));
  }

  private static Optional<List<Component>> sameIdentifiers(Collection<DatasetExpression> datasets) {
    Set<Set<Component>> identifiers = new LinkedHashSet<>();
    for (DatasetExpression dataset : datasets) {
      var ids = new LinkedHashSet<Component>();
      for (Component component : dataset.getDataStructure().values()) {
        if (component.getRole().equals(Dataset.Role.IDENTIFIER)) {
          ids.add(component);
        }
      }
      identifiers.add(ids);
    }
    if (identifiers.size() != 1) {
      return Optional.empty();
    }
    return Optional.of(new ArrayList<>(identifiers.iterator().next()));
  }
}

package fr.insee.vtl.engine.semantics.join;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.semantics.clause.ClauseExecutor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Applies VTL join-body clauses on the virtual join result (still possibly carrying {@code
 * alias#name} columns), before automatic alias removal.
 *
 * <p>Order per VTL 2.1 Join Behaviour: filter → apply|calc|aggr → keep|drop → rename.
 */
public final class JoinBodyExecutor {

  private JoinBodyExecutor() {}

  public static boolean isEmpty(VtlParser.JoinBodyContext body) {
    return body == null
        || (body.filterClause() == null
            && body.calcClause() == null
            && body.joinApplyClause() == null
            && body.aggrClause() == null
            && body.keepOrDropClause() == null
            && body.renameClause() == null);
  }

  public static DatasetExpression apply(
      DatasetExpression virtual,
      VtlParser.JoinBodyContext body,
      Collection<String> operandAliases,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine engine) {
    if (isEmpty(body)) {
      return virtual;
    }
    VtlScriptEngine scriptEngine = expressionVisitor.getEngine();
    DatasetExpression current = virtual;

    if (body.filterClause() != null) {
      current =
          ClauseExecutor.filter(
              current,
              body.filterClause(),
              componentVisitor(current, scriptEngine, engine),
              engine);
    }

    if (body.joinApplyClause() != null) {
      current =
          applyJoinApply(current, body.joinApplyClause(), operandAliases, scriptEngine, engine);
    } else if (body.calcClause() != null) {
      current =
          ClauseExecutor.calc(
              current, body.calcClause(), componentVisitor(current, scriptEngine, engine), engine);
    } else if (body.aggrClause() != null) {
      current =
          ClauseExecutor.aggr(
              current, body.aggrClause(), componentVisitor(current, scriptEngine, engine), engine);
    }

    if (body.keepOrDropClause() != null) {
      current = ClauseExecutor.keepOrDrop(current, body.keepOrDropClause(), engine);
    }

    if (body.renameClause() != null) {
      current = ClauseExecutor.rename(current, body.renameClause(), engine);
    }

    return current;
  }

  /**
   * Pairwise application of {@code applyExpr} on homonym measures ({@code alias#measure}), writing
   * bare measure names and dropping the aliased sources.
   */
  static DatasetExpression applyJoinApply(
      DatasetExpression virtual,
      VtlParser.JoinApplyClauseContext applyClause,
      Collection<String> operandAliases,
      VtlScriptEngine scriptEngine,
      ProcessingEngine engine) {
    List<String> aliases = new ArrayList<>(operandAliases);
    if (aliases.size() < 2) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "join apply requires at least two operands", fromContext(applyClause)));
    }

    DataStructure structure = virtual.getDataStructure();
    List<String> commonMeasures = commonAliasedMeasures(structure, aliases);
    if (commonMeasures.isEmpty()) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "join apply found no homonym measures across operands", fromContext(applyClause)));
    }

    Map<String, ResolvableExpression> calcs = new LinkedHashMap<>();
    Map<String, Dataset.Role> roles = new LinkedHashMap<>();
    Set<String> dropAliased = new LinkedHashSet<>();

    for (String measure : commonMeasures) {
      Map<String, Object> bindings = new HashMap<>();
      structure.values().forEach(c -> bindings.put(c.getName(), c));
      for (String alias : aliases) {
        String physical = alias + "#" + measure;
        Component component = structure.get(physical);
        if (component == null) {
          throw new VtlRuntimeException(
              new InvalidArgumentException(
                  "join apply missing component " + physical, fromContext(applyClause)));
        }
        bindings.put(alias, component);
        dropAliased.add(physical);
      }
      ExpressionVisitor visitor = new ExpressionVisitor(bindings, engine, scriptEngine);
      ResolvableExpression expr = visitor.visit(applyClause.expr());
      calcs.put(measure, expr);
      roles.put(measure, Dataset.Role.MEASURE);
    }

    DatasetExpression calculated = engine.executeCalc(virtual, calcs, roles, Map.of());
    List<String> keep =
        calculated.getColumnNames().stream().filter(name -> !dropAliased.contains(name)).toList();
    return engine.executeProject(calculated, keep);
  }

  private static List<String> commonAliasedMeasures(DataStructure structure, List<String> aliases) {
    Set<String> common = null;
    for (String alias : aliases) {
      Set<String> measures = new LinkedHashSet<>();
      String prefix = alias + "#";
      for (Component component : structure.getMeasures()) {
        String name = component.getName();
        if (name.startsWith(prefix)) {
          measures.add(name.substring(prefix.length()));
        }
      }
      if (common == null) {
        common = measures;
      } else {
        common.retainAll(measures);
      }
    }
    return common == null ? List.of() : new ArrayList<>(common);
  }

  private static ExpressionVisitor componentVisitor(
      DatasetExpression dataset, VtlScriptEngine scriptEngine, ProcessingEngine engine) {
    Map<String, Object> bindings = new HashMap<>();
    dataset.getDataStructure().values().forEach(c -> bindings.put(c.getName(), c));
    return new ExpressionVisitor(bindings, engine, scriptEngine);
  }
}

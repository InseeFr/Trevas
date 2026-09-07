package fr.insee.vtl.prov.extract;

import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import fr.insee.vtl.prov.extract.PendingOp.Aggr;
import fr.insee.vtl.prov.extract.PendingOp.Apply;
import fr.insee.vtl.prov.extract.PendingOp.Arithmetic;
import fr.insee.vtl.prov.extract.PendingOp.Calc;
import fr.insee.vtl.prov.extract.PendingOp.Check;
import fr.insee.vtl.prov.extract.PendingOp.CheckDatapoint;
import fr.insee.vtl.prov.extract.PendingOp.Drop;
import fr.insee.vtl.prov.extract.PendingOp.ExistsIn;
import fr.insee.vtl.prov.extract.PendingOp.Filter;
import fr.insee.vtl.prov.extract.PendingOp.Identity;
import fr.insee.vtl.prov.extract.PendingOp.Join;
import fr.insee.vtl.prov.extract.PendingOp.Keep;
import fr.insee.vtl.prov.extract.PendingOp.Membership;
import fr.insee.vtl.prov.extract.PendingOp.PassThrough;
import fr.insee.vtl.prov.extract.PendingOp.Pivot;
import fr.insee.vtl.prov.extract.PendingOp.Rename;
import fr.insee.vtl.prov.extract.PendingOp.SetOp;
import fr.insee.vtl.prov.extract.PendingOp.Sub;
import fr.insee.vtl.prov.extract.PendingOp.Unpivot;
import fr.insee.vtl.prov.ir.ProvGraph;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Adds provenance edges for a materialized dataset from its {@link PendingOp}. */
final class EdgeLinker {

  private final ProvGraph graph;
  private final Function<String, DataStructure> structures;

  EdgeLinker(ProvGraph graph, Function<String, DataStructure> structures) {
    this.graph = graph;
    this.structures = structures;
  }

  void link(PendingOp op, String outId, DataStructure outStructure) {
    if (op instanceof Identity id) {
      linkComponentWise(outId, outStructure, List.of(id.datasetId()), "assign");
      return;
    }
    if (op instanceof Arithmetic arithmetic) {
      linkComponentWise(outId, outStructure, arithmetic.operandIds(), arithmetic.op());
      return;
    }
    if (op instanceof Calc calc) {
      linkMappedExprs(outId, outStructure, calc.srcId(), calc.exprs(), "calc");
      return;
    }
    if (op instanceof Aggr aggr) {
      linkMappedExprs(outId, outStructure, aggr.srcId(), aggr.exprs(), "aggr");
      if (!aggr.havingExprIds().isEmpty()) {
        Map<String, String> condition = new LinkedHashMap<>(opEdge("aggr"));
        condition.put("role", "condition");
        for (String havingId : aggr.havingExprIds()) {
          graph.addEdge(outId, havingId, condition);
        }
      }
      return;
    }
    if (op instanceof Filter filter) {
      linkConditionClause(outId, outStructure, filter.srcId(), filter.conditionExprIds(), "filter");
      return;
    }
    if (op instanceof Sub sub) {
      linkConditionClause(outId, outStructure, sub.srcId(), sub.conditionExprIds(), "sub");
      return;
    }
    if (op instanceof Keep keep) {
      linkPassThroughAll(outId, outStructure, keep.srcId(), "keep");
      return;
    }
    if (op instanceof Drop drop) {
      linkPassThroughAll(outId, outStructure, drop.srcId(), "drop");
      return;
    }
    if (op instanceof Rename rename) {
      linkRename(outId, outStructure, rename.srcId(), rename.renameFrom());
      return;
    }
    if (op instanceof Join join) {
      linkJoin(outId, outStructure, join.operandIds(), join.op());
      return;
    }
    if (op instanceof SetOp setOp) {
      if ("setdiff".equals(setOp.op())) {
        linkSetDiff(outId, outStructure, setOp.operandIds().get(0), setOp.operandIds().get(1));
      } else {
        linkComponentWise(outId, outStructure, setOp.operandIds(), setOp.op());
      }
      return;
    }
    if (op instanceof CheckDatapoint check) {
      linkCheckDatapoint(outId, outStructure, check);
      return;
    }
    if (op instanceof Check check) {
      linkCheck(outId, outStructure, check);
      return;
    }
    if (op instanceof PassThrough pass) {
      linkPassThroughProducer(outId, outStructure, pass);
      return;
    }
    if (op instanceof ExistsIn existsIn) {
      linkExistsIn(outId, outStructure, existsIn);
      return;
    }
    if (op instanceof Pivot pivot) {
      linkPivot(outId, outStructure, pivot);
      return;
    }
    if (op instanceof Unpivot unpivot) {
      linkUnpivot(outId, outStructure, unpivot);
      return;
    }
    if (op instanceof Membership membership) {
      linkMembership(outId, outStructure, membership);
      return;
    }
    if (op instanceof Apply apply) {
      linkApply(outId, outStructure, apply);
      return;
    }
    throw new IllegalStateException("unhandled pending op " + op.getClass().getName());
  }

  private DataStructure require(String datasetId) {
    DataStructure structure = structures.apply(datasetId);
    if (structure == null) {
      throw new IllegalStateException("unknown structure for " + datasetId);
    }
    return structure;
  }

  private void linkPivot(String outId, DataStructure outStructure, Pivot pivot) {
    Map<String, String> edge = opEdge(pivot.op());
    Map<String, String> condition = new LinkedHashMap<>(edge);
    condition.put("role", "condition");
    graph.addEdge(outId, pivot.srcId(), edge);
    DataStructure src = require(pivot.srcId());
    Set<String> pivoted = new LinkedHashSet<>(pivot.pivotedColumns());
    for (Component component : outStructure.values()) {
      String name = component.getName();
      String outVar = outId + "." + name;
      if (pivoted.contains(name)) {
        graph.addEdge(outVar, pivot.srcId() + "." + pivot.measureComponent(), edge);
        graph.addEdge(outVar, pivot.srcId() + "." + pivot.idComponent(), condition);
      } else if (src.containsKey(name)) {
        graph.addEdge(outVar, pivot.srcId() + "." + name, edge);
      }
    }
  }

  private void linkUnpivot(String outId, DataStructure outStructure, Unpivot unpivot) {
    Map<String, String> edge = opEdge("unpivot");
    graph.addEdge(outId, unpivot.srcId(), edge);
    DataStructure src = require(unpivot.srcId());
    for (Component component : outStructure.values()) {
      String name = component.getName();
      String outVar = outId + "." + name;
      if (name.equals(unpivot.idComponent())) {
        // identifier values come from measure *names* — no variable dep
        continue;
      }
      if (name.equals(unpivot.measureComponent())) {
        for (Component srcComp : src.values()) {
          if (srcComp.isMeasure()) {
            graph.addEdge(outVar, unpivot.srcId() + "." + srcComp.getName(), edge);
          }
        }
      } else if (src.containsKey(name)) {
        graph.addEdge(outVar, unpivot.srcId() + "." + name, edge);
      }
    }
  }

  private void linkMembership(String outId, DataStructure outStructure, Membership membership) {
    Map<String, String> edge = opEdge("#");
    graph.addEdge(outId, membership.srcId(), edge);
    linkPassThrough(outId, outStructure, membership.srcId(), edge);
  }

  private void linkApply(String outId, DataStructure outStructure, Apply apply) {
    Map<String, String> edge = opEdge("apply");
    graph.addEdge(outId, apply.srcId(), edge);
    for (Component component : outStructure.values()) {
      String outVar = outId + "." + component.getName();
      if (component.getName().equals(apply.measureName())) {
        graph.addEdge(outVar, apply.exprId(), edge);
      } else {
        graph.addEdge(outVar, apply.srcId() + "." + component.getName(), edge);
      }
    }
  }

  private void linkCheckDatapoint(String outId, DataStructure outStructure, CheckDatapoint check) {
    Map<String, String> edge = opEdge("check_datapoint", "ruleset", check.ruleset());
    Map<String, String> pass = opEdge("check_datapoint");
    graph.addEdge(outId, check.srcId(), edge);
    Set<String> validationCols = Set.of("bool_var", "errorcode", "errorlevel");
    for (Component component : outStructure.values()) {
      String name = component.getName();
      if ("ruleid".equals(name)) {
        continue;
      }
      String outVar = outId + "." + name;
      if (validationCols.contains(name)) {
        for (String validated : check.validatedVars()) {
          graph.addEdge(outVar, check.srcId() + "." + validated, edge);
        }
      } else if (require(check.srcId()).containsKey(name)) {
        graph.addEdge(outVar, check.srcId() + "." + name, pass);
      }
    }
  }

  private void linkCheck(String outId, DataStructure outStructure, Check check) {
    Map<String, String> edge = opEdge("check");
    graph.addEdge(outId, check.srcId(), edge);
    if (check.imbalanceId() != null) {
      graph.addEdge(outId, check.imbalanceId(), edge);
    }
    DataStructure src = require(check.srcId());
    for (Component component : outStructure.values()) {
      String name = component.getName();
      String outVar = outId + "." + name;
      if ("imbalance".equals(name) && check.imbalanceId() != null) {
        DataStructure imbalance = require(check.imbalanceId());
        for (Component imb : imbalance.values()) {
          if (imb.isMeasure()) {
            graph.addEdge(outVar, check.imbalanceId() + "." + imb.getName(), edge);
            break;
          }
        }
      } else if (src.containsKey(name)) {
        graph.addEdge(outVar, check.srcId() + "." + name, edge);
      }
      // errorcode / errorlevel: no variable deps when literals omitted
    }
  }

  /**
   * Unary pass-through: optional ruleset on the dataset edge and on measures (hierarchy); plain
   * {@code op} when no ruleset (time-series, eval).
   */
  private void linkPassThroughProducer(String outId, DataStructure outStructure, PassThrough pass) {
    if (pass.ruleset() == null) {
      linkPassThroughAll(outId, outStructure, pass.srcId(), pass.op());
      return;
    }
    Map<String, String> annotated = opEdge(pass.op(), "ruleset", pass.ruleset());
    Map<String, String> plain = opEdge(pass.op());
    graph.addEdge(outId, pass.srcId(), annotated);
    DataStructure src = require(pass.srcId());
    for (Component component : outStructure.values()) {
      String name = component.getName();
      if (!src.containsKey(name)) {
        continue;
      }
      Map<String, String> edge = component.isMeasure() ? annotated : plain;
      graph.addEdge(outId + "." + name, pass.srcId() + "." + name, edge);
    }
  }

  private void linkPassThroughAll(
      String outId, DataStructure outStructure, String srcId, String op) {
    Map<String, String> edge = opEdge(op);
    graph.addEdge(outId, srcId, edge);
    linkPassThrough(outId, outStructure, srcId, edge);
  }

  private void linkSetDiff(
      String outId, DataStructure outStructure, String leftId, String rightId) {
    Map<String, String> edge = opEdge("setdiff");
    Map<String, String> condition = new LinkedHashMap<>(edge);
    condition.put("role", "condition");
    graph.addEdge(outId, leftId, edge);
    graph.addEdge(outId, rightId, condition);
    linkPassThrough(outId, outStructure, leftId, edge);
  }

  private void linkExistsIn(String outId, DataStructure outStructure, ExistsIn existsIn) {
    Map<String, String> edge = opEdge("exists_in");
    Map<String, String> condition = new LinkedHashMap<>(edge);
    condition.put("role", "condition");
    graph.addEdge(outId, existsIn.leftId(), edge);
    graph.addEdge(outId, existsIn.rightId(), condition);
    DataStructure left = require(existsIn.leftId());
    DataStructure right = require(existsIn.rightId());
    for (Component component : outStructure.values()) {
      String name = component.getName();
      String outVar = outId + "." + name;
      if ("bool_var".equals(name)) {
        for (Component id : left.getIdentifiers()) {
          graph.addEdge(outVar, existsIn.leftId() + "." + id.getName(), edge);
        }
        for (Component id : right.getIdentifiers()) {
          graph.addEdge(outVar, existsIn.rightId() + "." + id.getName(), condition);
        }
      } else if (left.containsKey(name)) {
        graph.addEdge(outVar, existsIn.leftId() + "." + name, edge);
      }
    }
  }

  private void linkJoin(
      String outId, DataStructure outStructure, List<String> operandIds, String op) {
    Map<String, String> edge = opEdge(op);
    for (String operandId : operandIds) {
      graph.addEdge(outId, operandId, edge);
    }
    for (Component component : outStructure.values()) {
      String name = component.getName();
      String outVar = outId + "." + name;
      for (String operandId : operandIds) {
        if (require(operandId).containsKey(name)) {
          graph.addEdge(outVar, operandId + "." + name, edge);
        }
      }
    }
  }

  private void linkMappedExprs(
      String outId,
      DataStructure outStructure,
      String srcId,
      Map<String, String> mappedExprs,
      String op) {
    Map<String, String> edge = opEdge(op);
    graph.addEdge(outId, srcId, edge);
    for (Component component : outStructure.values()) {
      String outVar = outId + "." + component.getName();
      String exprId = mappedExprs.get(component.getName());
      if (exprId != null) {
        graph.addEdge(outVar, exprId, edge);
      } else {
        graph.addEdge(outVar, srcId + "." + component.getName(), edge);
      }
    }
  }

  private void linkConditionClause(
      String outId,
      DataStructure outStructure,
      String srcId,
      List<String> conditionExprIds,
      String op) {
    Map<String, String> edge = opEdge(op);
    Map<String, String> condition = new LinkedHashMap<>(edge);
    condition.put("role", "condition");
    graph.addEdge(outId, srcId, edge);
    for (String exprId : conditionExprIds) {
      graph.addEdge(outId, exprId, condition);
    }
    linkPassThrough(outId, outStructure, srcId, edge);
  }

  private void linkRename(
      String outId, DataStructure outStructure, String srcId, Map<String, String> renameFrom) {
    Map<String, String> edge = opEdge("rename");
    graph.addEdge(outId, srcId, edge);
    for (Component component : outStructure.values()) {
      String srcComponent = renameFrom.getOrDefault(component.getName(), component.getName());
      graph.addEdge(outId + "." + component.getName(), srcId + "." + srcComponent, edge);
    }
  }

  private void linkComponentWise(
      String outId, DataStructure outStructure, List<String> operandIds, String op) {
    Map<String, String> edge = opEdge(op);
    for (String operandId : operandIds) {
      graph.addEdge(outId, operandId, edge);
    }
    for (Component component : outStructure.values()) {
      String outVar = outId + "." + component.getName();
      for (String operandId : operandIds) {
        graph.addEdge(outVar, operandId + "." + component.getName(), edge);
      }
    }
  }

  private void linkPassThrough(
      String outId, DataStructure outStructure, String srcId, Map<String, String> edge) {
    for (Component component : outStructure.values()) {
      graph.addEdge(outId + "." + component.getName(), srcId + "." + component.getName(), edge);
    }
  }

  private static Map<String, String> opEdge(String op) {
    return Map.of("op", op);
  }

  private static Map<String, String> opEdge(String op, String key, String value) {
    Map<String, String> edge = new LinkedHashMap<>();
    edge.put("op", op);
    edge.put(key, value);
    return edge;
  }
}

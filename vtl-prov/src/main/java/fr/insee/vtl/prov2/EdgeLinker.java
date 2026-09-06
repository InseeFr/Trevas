package fr.insee.vtl.prov2;

import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import fr.insee.vtl.prov2.PendingOp.Aggr;
import fr.insee.vtl.prov2.PendingOp.Apply;
import fr.insee.vtl.prov2.PendingOp.Arithmetic;
import fr.insee.vtl.prov2.PendingOp.Calc;
import fr.insee.vtl.prov2.PendingOp.CheckDatapoint;
import fr.insee.vtl.prov2.PendingOp.Drop;
import fr.insee.vtl.prov2.PendingOp.Filter;
import fr.insee.vtl.prov2.PendingOp.Identity;
import fr.insee.vtl.prov2.PendingOp.Join;
import fr.insee.vtl.prov2.PendingOp.Keep;
import fr.insee.vtl.prov2.PendingOp.Membership;
import fr.insee.vtl.prov2.PendingOp.Pivot;
import fr.insee.vtl.prov2.PendingOp.Rename;
import fr.insee.vtl.prov2.PendingOp.SetOp;
import fr.insee.vtl.prov2.PendingOp.Sub;
import fr.insee.vtl.prov2.PendingOp.Unpivot;
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
      linkConditionClause(
          outId, outStructure, filter.srcId(), filter.conditionExprIds(), "filter");
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
    Map<String, String> edge = new LinkedHashMap<>();
    edge.put("op", "check_datapoint");
    edge.put("ruleset", check.ruleset());
    Map<String, String> pass = Map.of("op", "check_datapoint");
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
}

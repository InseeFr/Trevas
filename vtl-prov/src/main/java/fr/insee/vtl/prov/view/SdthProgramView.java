package fr.insee.vtl.prov.view;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.prov.extract.VtlJavaTypes;
import fr.insee.vtl.prov.ir.ProvGraph;
import fr.insee.vtl.prov.prov.DataframeInstance;
import fr.insee.vtl.prov.prov.FileInstance;
import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.prov.ProgramStep;
import fr.insee.vtl.prov.prov.VariableInstance;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compatibility projection: {@link ProvGraph} → legacy {@link Program} for {@link
 * fr.insee.vtl.prov.utils.RDFUtils} (spec 20260808_01). Rolls up expression nodes and anonymous
 * intermediates; folds condition deps into {@code usesVariable}; fills SDTH entity lineage ({@code
 * wasDerivedFrom} / {@code elaborationOf} / {@code FileInstance}).
 */
public final class SdthProgramView {

  private static final Pattern STMT = Pattern.compile("^([^@#][^@]*)@(\\d+)$");

  private SdthProgramView() {}

  /**
   * @param programId stable program resource id (URI segment)
   * @param label human label ({@code rdfs:label})
   * @param sourceCode full script text (kept on {@link Program} for callers; not emitted on Program
   *     RDF — only on steps)
   */
  public static Program toProgram(
      ProvGraph graph, String programId, String label, String sourceCode) {
    Program program = new Program(programId, label);
    program.setSourceCode(sourceCode == null ? "" : sourceCode);

    Map<String, Map<String, String>> vertices = graph.vertices();
    Map<String, List<ProvGraph.Edge>> outEdges = indexOutEdges(graph);
    Map<String, List<String>> varsByDataset = indexVariablesByDataset(vertices);

    List<String> producedIds = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : vertices.entrySet()) {
      String id = entry.getKey();
      Map<String, String> attrs = entry.getValue();
      if (!"dataset".equals(attrs.get("kind"))) {
        continue;
      }
      if ("true".equals(attrs.get("anon"))) {
        continue;
      }
      Matcher m = STMT.matcher(id);
      if (m.matches() && Integer.parseInt(m.group(2)) > 0) {
        producedIds.add(id);
      }
    }
    producedIds.sort(
        Comparator.comparingInt((String id) -> statementIndex(id)).thenComparing(id -> id));

    Map<String, DataframeInstance> dataframes = new LinkedHashMap<>();
    Map<String, VariableInstance> variables = new LinkedHashMap<>();
    Map<String, FileInstance> files = new LinkedHashMap<>();
    int stepIndex = 1;
    for (String outId : producedIds) {
      Map<String, String> outAttrs = vertices.get(outId);
      String outLabel = ProvIds.bindingName(outId);
      String stepSrc = outAttrs.getOrDefault("src", outLabel);
      ProgramStep step = new ProgramStep(outLabel, stepSrc, stepIndex++);
      step.setId("step-" + outId);

      DataframeInstance produced =
          dataframe(dataframes, variables, outId, outLabel, vertices, varsByDataset);
      step.setProducedDataframe(produced);

      Set<String> consumedIds = new LinkedHashSet<>();
      Set<String> usedVarIds = new LinkedHashSet<>();
      Set<String> assignedVarIds = new LinkedHashSet<>();
      Set<String> rulesets = new LinkedHashSet<>();
      Set<String> seen = new HashSet<>();

      for (ProvGraph.Edge edge : outEdges.getOrDefault(outId, List.of())) {
        if (edge.attrs().get("ruleset") != null) {
          rulesets.add(edge.attrs().get("ruleset"));
        }
        collectFromNode(edge.to(), vertices, outEdges, consumedIds, usedVarIds, rulesets, seen);
      }
      for (String varId : variablesOf(outId, varsByDataset)) {
        for (ProvGraph.Edge edge : outEdges.getOrDefault(varId, List.of())) {
          String to = edge.to();
          Map<String, String> toAttrs = vertices.getOrDefault(to, Map.of());
          if ("expression".equals(toAttrs.get("kind"))) {
            assignedVarIds.add(varId);
            collectFromNode(to, vertices, outEdges, consumedIds, usedVarIds, rulesets, seen);
          } else {
            collectFromNode(to, vertices, outEdges, consumedIds, usedVarIds, rulesets, seen);
          }
          if (edge.attrs().get("ruleset") != null) {
            rulesets.add(edge.attrs().get("ruleset"));
          }
        }
      }

      for (String consumedId : consumedIds) {
        DataframeInstance consumed =
            dataframe(
                dataframes,
                variables,
                consumedId,
                ProvIds.bindingName(consumedId),
                vertices,
                varsByDataset);
        ensureRootFileLineage(consumed, consumedId, files, variables, vertices, varsByDataset);
        step.getConsumedDataframes().add(consumed);
      }

      boolean identityStep = isIdentityDatasetOp(outId, outEdges, vertices);
      if (identityStep && consumedIds.size() == 1) {
        String only = consumedIds.iterator().next();
        produced
            .getElaborationOfDataframes()
            .add(
                dataframe(
                    dataframes,
                    variables,
                    only,
                    ProvIds.bindingName(only),
                    vertices,
                    varsByDataset));
      } else {
        for (String consumedId : consumedIds) {
          produced
              .getWasDerivedFromDataframes()
              .add(
                  dataframe(
                      dataframes,
                      variables,
                      consumedId,
                      ProvIds.bindingName(consumedId),
                      vertices,
                      varsByDataset));
        }
      }

      linkVariableLineage(outId, variables, vertices, outEdges, varsByDataset);

      for (String varId : usedVarIds) {
        Map<String, String> attrs = vertices.get(varId);
        if (attrs == null) {
          continue;
        }
        String comp = ProvIds.componentName(varId);
        String parentId = attrs.get("dataset");
        String parentLabel = namedParentLabel(parentId, vertices, outEdges);
        VariableInstance used = variable(variables, varId, comp, attrs);
        used.setParentDataframe(parentLabel);
        step.getUsedVariables().add(used);
      }

      for (String varId : assignedVarIds) {
        Map<String, String> attrs = vertices.get(varId);
        String comp = ProvIds.componentName(varId);
        VariableInstance assigned = variable(variables, varId, comp, attrs);
        String exprSrc = assignedExpressionSrc(varId, outEdges, vertices);
        if (exprSrc != null) {
          assigned.setSourceCode(exprSrc.endsWith(";") ? exprSrc : exprSrc + ";");
        }
        assigned.setParentDataframe(outLabel);
        step.getAssignedVariables().add(assigned);
      }

      step.getRulesets().addAll(rulesets);
      program.getProgramSteps().add(step);
    }

    // Root inputs that never appear as consumed still get FileInstance lineage if present in IR.
    for (Map.Entry<String, Map<String, String>> entry : vertices.entrySet()) {
      String id = entry.getKey();
      Map<String, String> attrs = entry.getValue();
      if (!"dataset".equals(attrs.get("kind")) || "true".equals(attrs.get("anon"))) {
        continue;
      }
      Matcher m = STMT.matcher(id);
      if (m.matches() && Integer.parseInt(m.group(2)) == 0) {
        DataframeInstance root =
            dataframe(dataframes, variables, id, ProvIds.bindingName(id), vertices, varsByDataset);
        ensureRootFileLineage(root, id, files, variables, vertices, varsByDataset);
      }
    }
    return program;
  }

  private static void linkVariableLineage(
      String outDatasetId,
      Map<String, VariableInstance> variables,
      Map<String, Map<String, String>> vertices,
      Map<String, List<ProvGraph.Edge>> outEdges,
      Map<String, List<String>> varsByDataset) {
    for (String varId : variablesOf(outDatasetId, varsByDataset)) {
      VariableInstance outVar =
          variable(variables, varId, ProvIds.componentName(varId), vertices.get(varId));
      for (ProvGraph.Edge edge : outEdges.getOrDefault(varId, List.of())) {
        String to = edge.to();
        Map<String, String> toAttrs = vertices.getOrDefault(to, Map.of());
        String op = edge.attrs().get("op");
        if ("expression".equals(toAttrs.get("kind"))) {
          Set<String> leafVars = new LinkedHashSet<>();
          collectLeafVariables(to, vertices, outEdges, leafVars, new HashSet<>());
          for (String leafId : leafVars) {
            Map<String, String> leafAttrs = vertices.get(leafId);
            outVar
                .getWasDerivedFromVariables()
                .add(variable(variables, leafId, ProvIds.componentName(leafId), leafAttrs));
          }
        } else if ("variable".equals(toAttrs.get("kind"))) {
          VariableInstance parent = variable(variables, to, ProvIds.componentName(to), toAttrs);
          if ("assign".equals(op)
              || ProvIds.componentName(varId).equals(ProvIds.componentName(to))) {
            outVar.getElaborationOfVariables().add(parent);
          } else {
            outVar.getWasDerivedFromVariables().add(parent);
          }
        }
      }
    }
  }

  private static void collectLeafVariables(
      String nodeId,
      Map<String, Map<String, String>> vertices,
      Map<String, List<ProvGraph.Edge>> outEdges,
      Set<String> leafVars,
      Set<String> seen) {
    if (!seen.add(nodeId)) {
      return;
    }
    Map<String, String> attrs = vertices.getOrDefault(nodeId, Map.of());
    if ("variable".equals(attrs.get("kind"))) {
      leafVars.add(nodeId);
      return;
    }
    for (ProvGraph.Edge edge : outEdges.getOrDefault(nodeId, List.of())) {
      collectLeafVariables(edge.to(), vertices, outEdges, leafVars, seen);
    }
  }

  private static boolean isIdentityDatasetOp(
      String outId,
      Map<String, List<ProvGraph.Edge>> outEdges,
      Map<String, Map<String, String>> vertices) {
    boolean sawDataset = false;
    for (ProvGraph.Edge edge : outEdges.getOrDefault(outId, List.of())) {
      Map<String, String> toAttrs = vertices.getOrDefault(edge.to(), Map.of());
      if (!"dataset".equals(toAttrs.get("kind"))) {
        continue;
      }
      if ("true".equals(toAttrs.get("anon"))) {
        return false;
      }
      sawDataset = true;
      if (!"assign".equals(edge.attrs().get("op"))) {
        return false;
      }
    }
    return sawDataset;
  }

  private static void ensureRootFileLineage(
      DataframeInstance df,
      String versionedId,
      Map<String, FileInstance> files,
      Map<String, VariableInstance> variables,
      Map<String, Map<String, String>> vertices,
      Map<String, List<String>> varsByDataset) {
    Matcher m = STMT.matcher(versionedId);
    if (!m.matches() || Integer.parseInt(m.group(2)) != 0) {
      return;
    }
    if (!df.getWasDerivedFromFiles().isEmpty()) {
      return;
    }
    String name = ProvIds.bindingName(versionedId);
    FileInstance file = files.get(name);
    if (file == null) {
      file = new FileInstance(name);
      file.setId("file-" + name);
      for (String varId : variablesOf(versionedId, varsByDataset)) {
        Map<String, String> attrs = vertices.get(varId);
        // File shares the same variable instances as the root dataframe (same IR nodes).
        file.getHasVariableInstances()
            .add(variable(variables, varId, ProvIds.componentName(varId), attrs));
      }
      files.put(name, file);
    }
    df.getWasDerivedFromFiles().add(file);
  }

  private static void collectFromNode(
      String nodeId,
      Map<String, Map<String, String>> vertices,
      Map<String, List<ProvGraph.Edge>> outEdges,
      Set<String> consumedIds,
      Set<String> usedVarIds,
      Set<String> rulesets,
      Set<String> seen) {
    if (!seen.add(nodeId)) {
      return;
    }
    Map<String, String> attrs = vertices.getOrDefault(nodeId, Map.of());
    String kind = attrs.get("kind");
    if ("dataset".equals(kind)) {
      if ("true".equals(attrs.get("anon"))) {
        for (ProvGraph.Edge edge : outEdges.getOrDefault(nodeId, List.of())) {
          if (edge.attrs().get("ruleset") != null) {
            rulesets.add(edge.attrs().get("ruleset"));
          }
          collectFromNode(edge.to(), vertices, outEdges, consumedIds, usedVarIds, rulesets, seen);
        }
        return;
      }
      Matcher m = STMT.matcher(nodeId);
      if (m.matches()) {
        consumedIds.add(nodeId);
      }
      return;
    }
    if ("expression".equals(kind)) {
      for (ProvGraph.Edge edge : outEdges.getOrDefault(nodeId, List.of())) {
        String to = edge.to();
        Map<String, String> toAttrs = vertices.getOrDefault(to, Map.of());
        if ("variable".equals(toAttrs.get("kind"))) {
          usedVarIds.add(to);
        } else {
          collectFromNode(to, vertices, outEdges, consumedIds, usedVarIds, rulesets, seen);
        }
      }
      return;
    }
    if ("variable".equals(kind)) {
      return;
    }
  }

  private static String assignedExpressionSrc(
      String varId,
      Map<String, List<ProvGraph.Edge>> outEdges,
      Map<String, Map<String, String>> vertices) {
    for (ProvGraph.Edge edge : outEdges.getOrDefault(varId, List.of())) {
      Map<String, String> toAttrs = vertices.getOrDefault(edge.to(), Map.of());
      if ("expression".equals(toAttrs.get("kind"))) {
        return toAttrs.get("src");
      }
    }
    return null;
  }

  private static DataframeInstance dataframe(
      Map<String, DataframeInstance> cache,
      Map<String, VariableInstance> variables,
      String versionedId,
      String label,
      Map<String, Map<String, String>> vertices,
      Map<String, List<String>> varsByDataset) {
    DataframeInstance existing = cache.get(versionedId);
    if (existing != null) {
      return existing;
    }
    DataframeInstance df = new DataframeInstance(label);
    df.setId(versionedId);
    for (String varId : variablesOf(versionedId, varsByDataset)) {
      Map<String, String> attrs = vertices.get(varId);
      df.getHasVariableInstances()
          .add(variable(variables, varId, ProvIds.componentName(varId), attrs));
    }
    cache.put(versionedId, df);
    return df;
  }

  private static VariableInstance variable(
      Map<String, VariableInstance> cache, String varId, String label, Map<String, String> attrs) {
    VariableInstance existing = cache.get(varId);
    if (existing != null) {
      return existing;
    }
    VariableInstance v = new VariableInstance(label);
    v.setId(varId);
    applyRoleType(v, attrs);
    cache.put(varId, v);
    return v;
  }

  private static List<String> variablesOf(
      String datasetId, Map<String, List<String>> varsByDataset) {
    return varsByDataset.getOrDefault(datasetId, List.of());
  }

  private static Map<String, List<String>> indexVariablesByDataset(
      Map<String, Map<String, String>> vertices) {
    Map<String, List<String>> byDataset = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : vertices.entrySet()) {
      Map<String, String> attrs = entry.getValue();
      if (!"variable".equals(attrs.get("kind"))) {
        continue;
      }
      String datasetId = attrs.get("dataset");
      if (datasetId == null) {
        continue;
      }
      byDataset.computeIfAbsent(datasetId, k -> new ArrayList<>()).add(entry.getKey());
    }
    for (List<String> vars : byDataset.values()) {
      vars.sort(Comparator.naturalOrder());
    }
    return byDataset;
  }

  private static void applyRoleType(VariableInstance variable, Map<String, String> attrs) {
    if (attrs == null) {
      return;
    }
    String role = attrs.get("role");
    if (role != null) {
      variable.setRole(Dataset.Role.valueOf(role));
    }
    String type = attrs.get("type");
    if (type != null) {
      variable.setType(VtlJavaTypes.javaType(type));
    }
  }

  private static String namedParentLabel(
      String datasetId,
      Map<String, Map<String, String>> vertices,
      Map<String, List<ProvGraph.Edge>> outEdges) {
    if (datasetId == null) {
      return null;
    }
    if (!"true".equals(vertices.getOrDefault(datasetId, Map.of()).get("anon"))) {
      return ProvIds.bindingName(datasetId);
    }
    Set<String> seen = new HashSet<>();
    Deque<String> queue = new ArrayDeque<>();
    queue.add(datasetId);
    while (!queue.isEmpty()) {
      String id = queue.removeFirst();
      if (!seen.add(id)) {
        continue;
      }
      for (ProvGraph.Edge edge : outEdges.getOrDefault(id, List.of())) {
        String to = edge.to();
        Map<String, String> attrs = vertices.getOrDefault(to, Map.of());
        if ("dataset".equals(attrs.get("kind"))) {
          if ("true".equals(attrs.get("anon"))) {
            queue.add(to);
          } else {
            return ProvIds.bindingName(to);
          }
        }
      }
    }
    return datasetId;
  }

  private static Map<String, List<ProvGraph.Edge>> indexOutEdges(ProvGraph graph) {
    Map<String, List<ProvGraph.Edge>> out = new LinkedHashMap<>();
    for (ProvGraph.Edge edge : graph.edges()) {
      out.computeIfAbsent(edge.from(), k -> new ArrayList<>()).add(edge);
    }
    return out;
  }

  private static int statementIndex(String datasetId) {
    Matcher m = STMT.matcher(datasetId);
    return m.matches() ? Integer.parseInt(m.group(2)) : 0;
  }
}

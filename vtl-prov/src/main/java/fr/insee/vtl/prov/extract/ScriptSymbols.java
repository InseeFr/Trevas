package fr.insee.vtl.prov.extract;

import fr.insee.vtl.parser.VtlParser;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Script-level names collected during the support-check pass and reused by {@link
 * ProvenanceVisitor}: user-defined operators and ruleset signatures.
 *
 * <p>One instance per {@link ProvenanceExtractor#extract} — avoids registering the same defines
 * twice on separate visitor instances.
 */
final class ScriptSymbols {

  /**
   * {@code define operator}: formal parameter names (order = call args), which formals are
   * datasets, whether the operator returns a dataset, and the body AST for call-site inlining
   * (PR-39 scalar-in-calc; dataset producers via body visit).
   */
  record UserOperator(
      List<String> params,
      Set<String> datasetParams,
      boolean returnsDataset,
      VtlParser.ExprContext body) {}

  private final Map<String, UserOperator> userOperators = new LinkedHashMap<>();
  private final Map<String, List<String>> datapointRulesets = new LinkedHashMap<>();
  private final Set<String> hierarchicalRulesets = new LinkedHashSet<>();

  void putUserOperator(
      String name,
      List<String> params,
      Set<String> datasetParams,
      boolean returnsDataset,
      VtlParser.ExprContext body) {
    userOperators.put(
        name,
        new UserOperator(List.copyOf(params), Set.copyOf(datasetParams), returnsDataset, body));
  }

  boolean isUserOperator(String name) {
    return userOperators.containsKey(name);
  }

  UserOperator userOperator(String name) {
    return userOperators.get(name);
  }

  void putDatapointRuleset(String name, List<String> variables) {
    datapointRulesets.put(name, List.copyOf(variables));
  }

  /** Signature variable names, or {@code null} if the ruleset was never defined. */
  List<String> datapointVariables(String ruleset) {
    return datapointRulesets.get(ruleset);
  }

  void addHierarchicalRuleset(String name) {
    hierarchicalRulesets.add(name);
  }

  boolean isHierarchicalRuleset(String name) {
    return hierarchicalRulesets.contains(name);
  }
}

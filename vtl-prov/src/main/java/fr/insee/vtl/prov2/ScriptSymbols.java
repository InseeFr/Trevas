package fr.insee.vtl.prov2;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Script-level names collected during the support-check pass and reused by {@link
 * ProvenanceVisitor}: user-defined operators and datapoint ruleset signatures.
 *
 * <p>One instance per {@link ProvenanceExtractor#extract} — avoids registering the same defines
 * twice on separate visitor instances.
 */
final class ScriptSymbols {

  private final Set<String> userOperators = new LinkedHashSet<>();
  private final Map<String, List<String>> datapointRulesets = new LinkedHashMap<>();

  void addUserOperator(String name) {
    userOperators.add(name);
  }

  boolean isUserOperator(String name) {
    return userOperators.contains(name);
  }

  void putDatapointRuleset(String name, List<String> variables) {
    datapointRulesets.put(name, List.copyOf(variables));
  }

  /** Signature variable names, or {@code null} if the ruleset was never defined. */
  List<String> datapointVariables(String ruleset) {
    return datapointRulesets.get(ruleset);
  }
}

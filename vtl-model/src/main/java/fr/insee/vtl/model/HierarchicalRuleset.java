package fr.insee.vtl.model;

import java.util.List;

/**
 * Hierarchical rule set
 *
 * <p>The <code>HierarchicalRuleset</code> that contains Rules to be applied to individual
 * Components of a given Data Set in order to make validations or calculations according to
 * hierarchical relationships between the relevant Code Items.
 */
public class HierarchicalRuleset {

  private final List<HierarchicalRule> rules;
  private final String variable;
  private final Class<?> errorCodeType;
  private final Class<?> errorLevelType;

  /**
   * Constructor.
   *
   * @param rules List of rules.
   * @param variable Variable concerned.
   * @param errorCodeType Type of errorcode
   * @param errorLevelType Type of errorlevel
   */
  public HierarchicalRuleset(
      List<HierarchicalRule> rules,
      String variable,
      Class<?> errorCodeType,
      Class<?> errorLevelType) {
    this.rules = rules;
    this.variable = variable;
    this.errorCodeType = errorCodeType;
    this.errorLevelType = errorLevelType;
  }

  public List<HierarchicalRule> getRules() {
    return rules;
  }

  public String getVariable() {
    return variable;
  }

  public Class<?> getErrorCodeType() {
    return errorCodeType;
  }

  public Class<?> getErrorLevelType() {
    return errorLevelType;
  }
}

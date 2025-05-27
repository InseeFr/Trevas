package fr.insee.vtl.model;

import java.util.List;
import java.util.Map;

/**
 * Data point rule set
 *
 * <p>The <code>DataPointRuleset</code> contains Rules to be applied to each individual Data Point
 * of a Data Set for validation
 */
public class DataPointRuleset {

  private final String name;
  private final List<DataPointRule> rules;
  private List<String> variables;
  private final List<String> valuedomains;
  private final Map<String, String> alias;
  private final Class errorCodeType;
  private final Class errorLevelType;

  /**
   * Constructor.
   *
   * @param name Ruleset name.
   * @param rules List of rules.
   * @param variables List of variables.
   * @param valuedomains List of valuedomains.
   * @param alias Map of variable alias.
   * @param errorCodeType Type of errorcode
   * @param errorLevelType Type of errorlevel
   */
  public DataPointRuleset(
      String name,
      List<DataPointRule> rules,
      List<String> variables,
      List<String> valuedomains,
      Map<String, String> alias,
      Class errorCodeType,
      Class errorLevelType) {
    this.name = name;
    this.rules = rules;
    this.variables = variables;
    this.valuedomains = valuedomains;
    this.alias = alias;
    this.errorCodeType = errorCodeType;
    this.errorLevelType = errorLevelType;
  }

  public String getName() {
    return name;
  }

  public List<DataPointRule> getRules() {
    return rules;
  }

  public List<String> getVariables() {
    return variables;
  }

  public void setVariables(List<String> variables) {
    this.variables = variables;
  }

  public List<String> getValuedomains() {
    return valuedomains;
  }

  public Map<String, String> getAlias() {
    return alias;
  }

  public Class getErrorCodeType() {
    return errorCodeType;
  }

  public Class getErrorLevelType() {
    return errorLevelType;
  }
}

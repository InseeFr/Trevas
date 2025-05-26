package fr.insee.vtl.model;

import java.util.function.Function;

/**
 * Data point rule
 *
 * <p>The <code>DataPointRule</code> represent rule to be applied to each individual Data Point of a
 * Data Set for validation
 */
public class DataPointRule {

  private final String name;
  private final Function<Structured.DataStructure, ResolvableExpression> untypedAntecedentExpr;
  private final Function<Structured.DataStructure, ResolvableExpression> untypedConsequentExpr;
  private final ResolvableExpression errorCodeExpression;
  private final ResolvableExpression errorLevelExpression;

  /**
   * Constructor.
   *
   * @param name name of the rule
   * @param buildAntecedentExpression boolean expression to be evaluated for each single Data Point
   *     of the input Data Set. It can contain Values of the Value Domains or Variables specified in
   *     the Ruleset signature and constants; all the VTL-ML component level operators are allowed.
   *     If omitted then antecedentCondition is assumed to be TRUE.
   * @param buildConsequentExpression boolean expression to be evaluated for each single Data Point
   *     of the input Data Set when the antecedentCondition evaluates to TRUE (as mentioned, missing
   *     antecedent conditions are assumed to be TRUE). It contains Values of the Value Domains or
   *     Variables specified in the Ruleset signature and constants; all the VTL-ML component level
   *     operators are allowed. A consequent condition equal to FALSE is considered as a non valid
   *     result.
   * @param errorCodeExpression resolvable expression for the error code
   * @param errorLevelExpression resolvable expression for the error level (severity)
   */
  public <T> DataPointRule(
      String name,
      Function<Structured.DataStructure, ResolvableExpression> untypedAntecedentExpr,
      Function<Structured.DataStructure, ResolvableExpression> untypedConsequentExpr,
      ResolvableExpression errorCodeExpression,
      ResolvableExpression errorLevelExpression) {
    this.name = name;
    this.untypedAntecedentExpr = untypedAntecedentExpr;
    this.untypedConsequentExpr = untypedConsequentExpr;
    this.errorCodeExpression = errorCodeExpression;
    this.errorLevelExpression = errorLevelExpression;
  }

  public String getName() {
    return name;
  }

  public ResolvableExpression getBuildAntecedentExpression(Structured.DataStructure dataStructure) {
    return this.untypedAntecedentExpr.apply(dataStructure);
  }

  public ResolvableExpression getBuildConsequentExpression(Structured.DataStructure dataStructure) {
    return this.untypedConsequentExpr.apply(dataStructure);
  }

  public ResolvableExpression getErrorCodeExpression() {
    return errorCodeExpression;
  }

  public ResolvableExpression getErrorLevelExpression() {
    return errorLevelExpression;
  }
}

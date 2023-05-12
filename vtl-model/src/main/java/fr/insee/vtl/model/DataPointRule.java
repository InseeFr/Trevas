package fr.insee.vtl.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Data point rule
 * <p>
 * The <code>DataPointRule</code> represent rule to be applied to each individual Data Point of a Data Set for validation
 */

public class DataPointRule implements Serializable {

    private final String name;
    private final VtlFunction<Map<String, Object>, ResolvableExpression> buildAntecedentExpression;
    private final VtlFunction<Map<String, Object>, ResolvableExpression> buildConsequentExpression;
    private final ResolvableExpression errorCodeExpression;
    private final ResolvableExpression errorLevelExpression;

    /**
     * Constructor.
     *
     * @param name                      name of the rule
     * @param buildAntecedentExpression boolean expression to be evaluated for each single Data Point of the input Data Set. It
     *                                  can contain Values of the Value Domains or Variables specified in the Ruleset signature
     *                                  and constants; all the VTL-ML component level operators are allowed. If omitted then
     *                                  antecedentCondition is assumed to be TRUE.
     * @param buildConsequentExpression boolean expression to be evaluated for each single Data Point of the input Data Set when
     *                                  the antecedentCondition evaluates to TRUE (as mentioned, missing antecedent
     *                                  conditions are assumed to be TRUE). It contains Values of the Value Domains or Variables
     *                                  specified in the Ruleset signature and constants; all the VTL-ML component level
     *                                  operators are allowed. A consequent condition equal to FALSE is considered as a non valid result.
     * @param errorCodeExpression       resolvable expression for the error code
     * @param errorLevelExpression      resolvable expression for the error level (severity)
     */

    public <T> DataPointRule(String name,
                             VtlFunction<Map<String, Object>, ResolvableExpression> buildAntecedentExpression,
                             VtlFunction<Map<String, Object>, ResolvableExpression> buildConsequentExpression,
                             ResolvableExpression errorCodeExpression,
                             ResolvableExpression errorLevelExpression) {
        this.name = name;
        this.buildAntecedentExpression = buildAntecedentExpression;
        this.buildConsequentExpression = buildConsequentExpression;
        this.errorCodeExpression = errorCodeExpression;
        this.errorLevelExpression = errorLevelExpression;
    }

    public String getName() {
        return name;
    }

    public VtlFunction<Map<String, Object>, ResolvableExpression> getBuildAntecedentExpression() {
        return buildAntecedentExpression;
    }

    public VtlFunction<Map<String, Object>, ResolvableExpression> getBuildConsequentExpression() {
        return buildConsequentExpression;
    }

    public ResolvableExpression getErrorCodeExpression() {
        return errorCodeExpression;
    }

    public ResolvableExpression getErrorLevelExpression() {
        return errorLevelExpression;
    }
}
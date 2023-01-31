package fr.insee.vtl.model;

import java.util.Map;
import java.util.function.Function;

/**
 * Data point rule
 * <p>
 * The <code>DataPointRule</code> represent rule to be applied to each individual Data Point of a Data Set for validation
 */

public class DataPointRule {

    private final Function<Map<String, Object>, ResolvableExpression> buildAntecedentExpression;
    private final Function<Map<String, Object>, ResolvableExpression> buildConsequentExpression;
    private final String errorCode;
    private final String errorLevel;

    /**
     * Constructor.
     *
     * @param buildAntecedentExpression boolean expression to be evaluated for each single Data Point of the input Data Set. It
     *                                  can contain Values of the Value Domains or Variables specified in the Ruleset signature
     *                                  and constants; all the VTL-ML component level operators are allowed. If omitted then
     *                                  antecedentCondition is assumed to be TRUE.
     * @param buildConsequentExpression boolean expression to be evaluated for each single Data Point of the input Data Set when
     *                                  the antecedentCondition evaluates to TRUE (as mentioned, missing antecedent
     *                                  conditions are assumed to be TRUE). It contains Values of the Value Domains or Variables
     *                                  specified in the Ruleset signature and constants; all the VTL-ML component level
     *                                  operators are allowed. A consequent condition equal to FALSE is considered as a non valid result.
     * @param errorCode                 literal denoting the error code
     * @param errorLevel                literal denoting the error level (severity)
     */

    public <T> DataPointRule(Function<Map<String, Object>, ResolvableExpression> buildAntecedentExpression,
                             Function<Map<String, Object>, ResolvableExpression> buildConsequentExpression,
                             String errorCode,
                             String errorLevel) {
        this.buildAntecedentExpression = buildAntecedentExpression;
        this.buildConsequentExpression = buildConsequentExpression;
        this.errorCode = errorCode;
        this.errorLevel = errorLevel;
    }

    public Function<Map<String, Object>, ResolvableExpression> getBuildAntecedentExpression() {
        return buildAntecedentExpression;
    }

    public Function<Map<String, Object>, ResolvableExpression> getBuildConsequentExpression() {
        return buildConsequentExpression;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorLevel() {
        return errorLevel;
    }
}

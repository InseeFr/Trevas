package fr.insee.vtl.model;

import fr.insee.vtl.parser.VtlParser;

/**
 * Data point rule
 * <p>
 * The <code>DataPointRule</code> represent rule to be applied to each individual Data Point of a Data Set for validation
 */

public class DataPointRule {

    private final VtlParser.ExprContext antecedentCondition;
    private final VtlParser.ExprContext consequentCondition;
    private final String errorCode;
    private final String errorLevel;

    /**
     * Constructor.
     *
     * @param antecedentCondition boolean expression to be evaluated for each single Data Point of the input Data Set. It
     *                            can contain Values of the Value Domains or Variables specified in the Ruleset signature
     *                            and constants; all the VTL-ML component level operators are allowed. If omitted then
     *                            antecedentCondition is assumed to be TRUE.
     * @param consequentCondition boolean expression to be evaluated for each single Data Point of the input Data Set when
     *                            the antecedentCondition evaluates to TRUE (as mentioned, missing antecedent
     *                            conditions are assumed to be TRUE). It contains Values of the Value Domains or Variables
     *                            specified in the Ruleset signature and constants; all the VTL-ML component level
     *                            operators are allowed. A consequent condition equal to FALSE is considered as a non valid result.
     * @param errorCode           literal denoting the error code
     * @param errorLevel          literal denoting the error level (severity)
     */

    public <T> DataPointRule(VtlParser.ExprContext antecedentCondition,
                             VtlParser.ExprContext consequentCondition,
                             String errorCode,
                             String errorLevel) {
        this.antecedentCondition = antecedentCondition;
        this.consequentCondition = consequentCondition;
        this.errorCode = errorCode;
        this.errorLevel = errorLevel;
    }
}

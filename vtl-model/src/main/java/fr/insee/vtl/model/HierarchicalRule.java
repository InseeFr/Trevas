package fr.insee.vtl.model;

/**
 * Hierarchical rule
 * <p>
 * The <code>HierarchicalRule</code> represent a single hierarchical rule
 */

public class HierarchicalRule {

    private final String name;
    private final String valueDomainValue;
    private final ResolvableExpression expression;
    private final ResolvableExpression errorCodeExpression;
    private final ResolvableExpression errorLevelExpression;

    /**
     * Constructor.
     *
     * @param name                 name of the rule
     * @param valueDomainValue     name of the variable to consider
     * @param expression           VTL expression to eval for validation
     * @param errorCodeExpression  resolvable expression for the error code
     * @param errorLevelExpression resolvable expression for the error level (severity)
     */

    public <T> HierarchicalRule(String name,
                                String valueDomainValue,
                                ResolvableExpression expression,
                                ResolvableExpression errorCodeExpression,
                                ResolvableExpression errorLevelExpression) {
        this.name = name;
        this.valueDomainValue = valueDomainValue;
        this.expression = expression;
        this.errorCodeExpression = errorCodeExpression;
        this.errorLevelExpression = errorLevelExpression;
    }

    public String getName() {
        return name;
    }

    public String getValueDomainValue() {
        return valueDomainValue;
    }

    public ResolvableExpression getExpression() {
        return expression;
    }

    public ResolvableExpression getErrorCodeExpression() {
        return errorCodeExpression;
    }

    public ResolvableExpression getErrorLevelExpression() {
        return errorLevelExpression;
    }
}
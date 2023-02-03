package fr.insee.vtl.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Data point rule set
 * <p>
 * The <code>DataPointRuleset</code> contains Rules to be applied to each individual Data Point of a Data Set for validation
 */

public class DataPointRuleset implements Serializable {

    private final String name;
    private final List<DataPointRule> rules;
    private final List<String> variables;
    private final Map<String, String> alias;
    private final Class errorCodeType;
    private final Class errorLevelType;

    /**
     * Constructor.
     *
     * @param name           Ruleset name.
     * @param rules          List of rules.
     * @param variables      List of rules.
     * @param alias          Map of variable alias.
     * @param errorCodeType  Type of errorcode
     * @param errorLevelType Type of errorlevel
     */
    public DataPointRuleset(String name, List<DataPointRule> rules, List<String> variables,
                            Map<String, String> alias, Class errorCodeType, Class errorLevelType) {
        this.name = name;
        this.rules = rules;
        this.variables = variables;
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

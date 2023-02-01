package fr.insee.vtl.model;

import java.util.List;
import java.util.Map;

/**
 * Data point rule set
 * <p>
 * The <code>DataPointRuleset</code> contains Rules to be applied to each individual Data Point of a Data Set for validation
 */

public class DataPointRuleset {

    private final String name;
    private final List<DataPointRule> rules;
    private final List<String> variables;
    private final Map<String, String> alias;

    /**
     * Constructor.
     *
     * @param name      Ruleset name.
     * @param rules     List of rules.
     * @param variables List of rules.
     * @param alias Map of variable alias.
     *
     */
    public DataPointRuleset(String name, List<DataPointRule> rules, List<String> variables, Map<String, String> alias) {
        this.name = name;
        this.rules = rules;
        this.variables = variables;
        this.alias = alias;
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

    public Map<String, String> getAlias() {return alias;}
}

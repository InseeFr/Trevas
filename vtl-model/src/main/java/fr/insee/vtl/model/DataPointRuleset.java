package fr.insee.vtl.model;

import java.util.List;

/**
 * Data point rule set
 * <p>
 * The <code>DataPointRuleset</code> contains Rules to be applied to each individual Data Point of a Data Set for validation
 */

public class DataPointRuleset {

    private final String name;
    private final List<DataPointRule> rules;
    private final List<String> variables;

    /**
     * Constructor.
     *
     * @param name      Ruleset name.
     * @param rules     List of rules.
     * @param variables List of rules.
     */
    public DataPointRuleset(String name, List<DataPointRule> rules, List<String> variables) {
        this.name = name;
        this.rules = rules;
        this.variables = variables;
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
}

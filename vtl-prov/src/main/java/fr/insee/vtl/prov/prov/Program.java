package fr.insee.vtl.prov.prov;

import java.util.LinkedHashSet;

/* Filled thanks to listener, except for dataframInstances */
public class Program {

    String id;
    String label;
    LinkedHashSet<ProgramStep> programSteps = new LinkedHashSet<>();
    String sourceCode;

    public Program() {
    }

    public Program(String id, String label) {
        this.id = id;
        this.label = label;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public LinkedHashSet<ProgramStep> getProgramSteps() {
        return programSteps;
    }

    public String getSourceCode() {
        return sourceCode;
    }

    public void setSourceCode(String sourceCode) {
        this.sourceCode = sourceCode;
    }

    public ProgramStep getProgramStepByLabel(String label) {
        return programSteps.stream().filter(p -> p.getLabel().equals(label)).findFirst().orElse(null);
    }
}

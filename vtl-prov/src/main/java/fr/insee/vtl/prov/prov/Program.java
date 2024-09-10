package fr.insee.vtl.prov.prov;

import java.util.HashSet;
import java.util.Set;

public class Program {

    String id;
    String label;
    Set<String> programStepIds = new HashSet<>();

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

    public Set<String> getProgramStepIds() {
        return programStepIds;
    }

    public void setProgramSteps(Set<String> programStepIds) {
        this.programStepIds = programStepIds;
    }

    public String getSourceCode() {
        return sourceCode;
    }

    public void setSourceCode(String sourceCode) {
        this.sourceCode = sourceCode;
    }
}

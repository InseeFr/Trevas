package fr.insee.vtl.prov.prov;

public class ProgramStep {

    String label;
    String sourceCode;

    public ProgramStep() {
    }

    public ProgramStep(String label, String sourceCode) {
        this.label = label;
        this.sourceCode = sourceCode;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getSourceCode() {
        return sourceCode;
    }

    public void setSourceCode(String sourceCode) {
        this.sourceCode = sourceCode;
    }
}

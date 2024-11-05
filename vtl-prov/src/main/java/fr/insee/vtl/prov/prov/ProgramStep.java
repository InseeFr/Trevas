package fr.insee.vtl.prov.prov;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class ProgramStep {

    String id;
    String label;
    String sourceCode;
    int index;
    Set<VariableInstance> usedVariables = new HashSet<>();
    Set<VariableInstance> assignedVariables = new HashSet<>();
    Set<DataframeInstance> consumedDataframe = new HashSet<>();
    DataframeInstance producedDataframe;

    public ProgramStep(String label, String sourceCode, int index) {
        this.id = UUID.randomUUID().toString();
        this.label = label;
        this.sourceCode = sourceCode;
        this.index = index;
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

    public String getSourceCode() {
        return sourceCode;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setSourceCode(String sourceCode) {
        this.sourceCode = sourceCode;
    }

    public Set<VariableInstance> getUsedVariables() {
        return usedVariables;
    }

    public void setUsedVariables(Set<VariableInstance> usedVariables) {
        this.usedVariables = usedVariables;
    }

    public Set<VariableInstance> getAssignedVariables() {
        return assignedVariables;
    }

    public void setAssignedVariables(Set<VariableInstance> assignedVariables) {
        this.assignedVariables = assignedVariables;
    }

    public Set<DataframeInstance> getConsumedDataframe() {
        return consumedDataframe;
    }

    public void setConsumedDataframe(Set<DataframeInstance> consumedDataframe) {
        this.consumedDataframe = consumedDataframe;
    }

    public DataframeInstance getProducedDataframe() {
        return producedDataframe;
    }

    public void setProducedDataframe(DataframeInstance producedDataframe) {
        this.producedDataframe = producedDataframe;
    }
}

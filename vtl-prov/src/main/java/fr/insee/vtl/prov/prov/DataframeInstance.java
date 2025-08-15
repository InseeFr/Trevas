package fr.insee.vtl.prov.prov;

import fr.insee.vtl.prov.utils.ProvenanceUtils;

import java.util.HashSet;
import java.util.Set;

public class DataframeInstance {
    String id;
    String label;
    Set<VariableInstance> hasVariableInstances = new HashSet<>();

    public DataframeInstance(String label) {
        this.id = ProvenanceUtils.generateUUID();
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

    public Set<VariableInstance> getHasVariableInstances() {
        return hasVariableInstances;
    }

    public void setHasVariableInstances(Set<VariableInstance> hasVariableInstances) {
        this.hasVariableInstances = hasVariableInstances;
    }
}

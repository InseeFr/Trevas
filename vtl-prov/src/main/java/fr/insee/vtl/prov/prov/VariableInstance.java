package fr.insee.vtl.prov.prov;

import fr.insee.vtl.model.Dataset;

import java.util.UUID;

public class VariableInstance {
    String id;
    String label;
    Dataset.Role role;
    String parentDataframe;
    Class<?> type;

    public VariableInstance(String label) {
        this.id = UUID.randomUUID().toString();
        this.label = label;
    }

    public VariableInstance(String id, String label) {
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

    public Dataset.Role getRole() {
        return role;
    }

    public void setRole(Dataset.Role role) {
        this.role = role;
    }

    public Class<?> getType() {
        return type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }


    public String getParentDataframe() {
        return parentDataframe;
    }

    public void setParentDataframe(String parentDataframe) {
        this.parentDataframe = parentDataframe;
    }
}

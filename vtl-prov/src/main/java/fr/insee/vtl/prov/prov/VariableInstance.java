package fr.insee.vtl.prov.prov;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.prov.utils.ProvenanceUtils;
import java.util.LinkedHashSet;
import java.util.Set;

public class VariableInstance {
  String id;
  String label;
  Dataset.Role role;
  String parentDataframe;
  Class<?> type;
  String sourceCode;

  /** Value transform lineage (SDTH {@code wasDerivedFrom}). */
  Set<VariableInstance> wasDerivedFromVariables = new LinkedHashSet<>();

  /** Pass-through / same-entity lineage (SDTH {@code elaborationOf}). */
  Set<VariableInstance> elaborationOfVariables = new LinkedHashSet<>();

  public VariableInstance(String label) {
    this.id = ProvenanceUtils.generateUUID();
    this.label = label;
  }

  public VariableInstance(String label, String sourceCode) {
    this.id = ProvenanceUtils.generateUUID();
    this.label = label;
    this.sourceCode = sourceCode + ";";
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

  public String getSourceCode() {
    return sourceCode;
  }

  public void setSourceCode(String sourceCode) {
    this.sourceCode = sourceCode;
  }

  public Set<VariableInstance> getWasDerivedFromVariables() {
    return wasDerivedFromVariables;
  }

  public Set<VariableInstance> getElaborationOfVariables() {
    return elaborationOfVariables;
  }
}

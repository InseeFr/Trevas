package fr.insee.vtl.prov.prov;

import fr.insee.vtl.prov.utils.ProvenanceUtils;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class DataframeInstance {
  String id;
  String label;
  Set<VariableInstance> hasVariableInstances = new HashSet<>();

  /** Transform lineage (SDTH {@code wasDerivedFrom} → dataframe). */
  Set<DataframeInstance> wasDerivedFromDataframes = new LinkedHashSet<>();

  /** External load lineage (SDTH {@code wasDerivedFrom} → file). */
  Set<FileInstance> wasDerivedFromFiles = new LinkedHashSet<>();

  /** Identity / version elaboration (SDTH {@code elaborationOf} → dataframe). */
  Set<DataframeInstance> elaborationOfDataframes = new LinkedHashSet<>();

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

  public Set<DataframeInstance> getWasDerivedFromDataframes() {
    return wasDerivedFromDataframes;
  }

  public Set<FileInstance> getWasDerivedFromFiles() {
    return wasDerivedFromFiles;
  }

  public Set<DataframeInstance> getElaborationOfDataframes() {
    return elaborationOfDataframes;
  }
}

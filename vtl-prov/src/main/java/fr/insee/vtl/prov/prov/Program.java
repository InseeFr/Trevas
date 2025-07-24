package fr.insee.vtl.prov.prov;

import java.util.LinkedHashSet;
import java.util.Set;

/* Filled thanks to listener, except for dataframInstances */
public class Program {

  String id;
  String label;
  Set<ProgramStep> programSteps = new LinkedHashSet<>();

  /* Provided running preview mode */
  Set<DataframeInstance> dataframeInstances = new LinkedHashSet<>();

  String sourceCode;

  public Program() {}

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

  public Set<ProgramStep> getProgramSteps() {
    return programSteps;
  }

  public void setProgramSteps(Set<ProgramStep> programSteps) {
    this.programSteps = programSteps;
  }

  public Set<DataframeInstance> getDataframeInstances() {
    return dataframeInstances;
  }

  public void setDataframeInstances(Set<DataframeInstance> dataframeInstances) {
    this.dataframeInstances = dataframeInstances;
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

  public ProgramStep getProgramStepByIndex(int index) {
    return programSteps.stream().filter(p -> p.getIndex() == index).findFirst().orElse(null);
  }
}

package fr.insee.vtl.coverage.model;

import fr.insee.vtl.model.Dataset;
import java.util.Map;

public class Test {

  private String script;
  private Map<String, Dataset> input;
  private Map<String, Dataset> outputs;

  public String getScript() {
    return script;
  }

  public void setScript(String script) {
    this.script = script;
  }

  public Map<String, Dataset> getInput() {
    return input;
  }

  public void setInput(Map<String, Dataset> input) {
    this.input = input;
  }

  public Map<String, Dataset> getOutputs() {
    return outputs;
  }

  public void setOutputs(Map<String, Dataset> outputs) {
    this.outputs = outputs;
  }
}

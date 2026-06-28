package fr.insee.vtl.coverage.model;

import fr.insee.vtl.csv.DatasetConsistencyIssue;
import fr.insee.vtl.model.Dataset;
import java.util.List;
import java.util.Map;

public class Test {

  private String script;
  private Map<String, Dataset> input;
  private Map<String, Dataset> outputs;
  private List<DatasetConsistencyIssue> inputFixtureIssues = List.of();
  private List<DatasetConsistencyIssue> outputFixtureIssues = List.of();

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

  public List<DatasetConsistencyIssue> getInputFixtureIssues() {
    return inputFixtureIssues;
  }

  public void setInputFixtureIssues(List<DatasetConsistencyIssue> inputFixtureIssues) {
    this.inputFixtureIssues =
        inputFixtureIssues == null ? List.of() : List.copyOf(inputFixtureIssues);
  }

  public List<DatasetConsistencyIssue> getOutputFixtureIssues() {
    return outputFixtureIssues;
  }

  public void setOutputFixtureIssues(List<DatasetConsistencyIssue> outputFixtureIssues) {
    this.outputFixtureIssues =
        outputFixtureIssues == null ? List.of() : List.copyOf(outputFixtureIssues);
  }
}

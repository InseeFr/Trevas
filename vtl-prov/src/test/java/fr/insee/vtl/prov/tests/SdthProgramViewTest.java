package fr.insee.vtl.prov.tests;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.prov.prov.DataframeInstance;
import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.prov.ProgramStep;
import fr.insee.vtl.prov.prov.VariableInstance;
import fr.insee.vtl.prov.utils.RDFUtils;
import fr.insee.vtl.prov.ir.ProvGraph;
import fr.insee.vtl.prov.extract.ProvenanceExtractor;
import fr.insee.vtl.prov.view.SdthProgramView;
import fr.insee.vtl.testutils.InputDirectives;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Test;

/** PR-15: ProvGraph → Program → RDFUtils compatibility view (spec 20260808_01). */
class SdthProgramViewTest {

  private static final ProvenanceExtractor EXTRACTOR = new ProvenanceExtractor();
  private static final String SDTH = "http://rdf-vocabulary.ddialliance.org/sdth#";

  @Test
  void identityAssignment_oneStepConsumesInput() throws Exception {
    Program program = programFromCorpus("01-assignment");
    assertThat(program.getProgramSteps()).hasSize(1);
    ProgramStep step = onlyStep(program);
    assertThat(step.getProducedDataframe().getLabel()).isEqualTo("ds2");
    assertThat(labels(step.getConsumedDataframes())).containsExactly("ds1");
    assertThat(step.getAssignedVariables()).isEmpty();
    assertThat(step.getUsedVariables()).isEmpty();
    assertThat(componentLabels(step.getProducedDataframe())).contains("id", "var1", "var2");
  }

  @Test
  void calc_assignsTargetAndUsesRhsVars() throws Exception {
    Program program = programFromCorpus("03-calc");
    ProgramStep step = onlyStep(program);
    assertThat(labels(step.getConsumedDataframes())).containsExactly("ds1");
    assertThat(varLabels(step.getAssignedVariables())).containsExactly("var_sum");
    assertThat(varLabels(step.getUsedVariables())).containsExactlyInAnyOrder("var1", "var2");
    VariableInstance assigned = step.getAssignedVariables().iterator().next();
    assertThat(assigned.getSourceCode()).isEqualTo("var1 + var2;");
  }

  @Test
  void filter_usesConditionVars() throws Exception {
    Program program = programFromCorpus("04-filter");
    ProgramStep step = onlyStep(program);
    assertThat(labels(step.getConsumedDataframes())).containsExactly("ds1");
    assertThat(step.getAssignedVariables()).isEmpty();
    assertThat(varLabels(step.getUsedVariables())).contains("var1");
  }

  @Test
  void chainFilterCalc_rollsUpAnonIntermediate() throws Exception {
    Program program = programFromCorpus("chain-filter-calc");
    assertThat(program.getProgramSteps()).hasSize(1);
    ProgramStep step = onlyStep(program);
    assertThat(step.getProducedDataframe().getLabel()).isEqualTo("ds_res");
    assertThat(labels(step.getConsumedDataframes())).containsExactly("ds_mul");
    assertThat(varLabels(step.getAssignedVariables())).containsExactly("var_sum");
    assertThat(varLabels(step.getUsedVariables())).contains("var1", "var2");
  }

  @Test
  void arithmetic_consumesBothOperands() throws Exception {
    Program program = programFromCorpus("02-arithmetic");
    ProgramStep step = onlyStep(program);
    assertThat(labels(step.getConsumedDataframes())).containsExactlyInAnyOrder("ds1", "ds2");
    assertThat(step.getAssignedVariables()).isEmpty();
    assertThat(step.getUsedVariables()).isEmpty();
  }

  @Test
  void rdfUtils_emitsSdthTypes() throws Exception {
    Program program = programFromCorpus("03-calc");
    program.setId("trevas-view-test");
    program.setLabel("SdthProgramView test");
    Model model = RDFUtils.buildModel(program);
    assertThat(model.size()).isPositive();
    assertThat(typed(model, SDTH + "Program")).isNotEmpty();
    assertThat(typed(model, SDTH + "ProgramStep")).hasSize(1);
    assertThat(typed(model, SDTH + "DataframeInstance")).isNotEmpty();
    assertThat(typed(model, SDTH + "VariableInstance")).isNotEmpty();
  }

  private static Program programFromCorpus(String folder) throws Exception {
    Path dir = corpusDir().resolve(folder);
    String script = Files.readString(dir.resolve("input.vtl"));
    ProvGraph graph = EXTRACTOR.extract(script, InputDirectives.parse(script));
    return SdthProgramView.toProgram(graph, "test-" + folder, folder, script);
  }

  private static Path corpusDir() {
    for (Path candidate : List.of(Path.of("tests"), Path.of("vtl-prov", "tests"))) {
      if (Files.isDirectory(candidate)) {
        return candidate;
      }
    }
    throw new IllegalStateException("corpus directory not found");
  }

  private static ProgramStep onlyStep(Program program) {
    assertThat(program.getProgramSteps()).hasSize(1);
    return program.getProgramSteps().iterator().next();
  }

  private static Set<String> labels(Set<DataframeInstance> dfs) {
    return dfs.stream().map(DataframeInstance::getLabel).collect(Collectors.toSet());
  }

  private static Set<String> varLabels(Set<VariableInstance> vars) {
    return vars.stream().map(VariableInstance::getLabel).collect(Collectors.toSet());
  }

  private static Set<String> componentLabels(DataframeInstance df) {
    return df.getHasVariableInstances().stream()
        .map(VariableInstance::getLabel)
        .collect(Collectors.toSet());
  }

  private static List<Resource> typed(Model model, String typeUri) {
    return model.listResourcesWithProperty(RDF.type, model.createResource(typeUri)).toList();
  }
}

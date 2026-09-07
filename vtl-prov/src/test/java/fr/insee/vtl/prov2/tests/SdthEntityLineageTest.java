package fr.insee.vtl.prov2.tests;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.prov.prov.DataframeInstance;
import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.prov.ProgramStep;
import fr.insee.vtl.prov.prov.VariableInstance;
import fr.insee.vtl.prov.utils.RDFUtils;
import fr.insee.vtl.prov2.ProvGraph;
import fr.insee.vtl.prov2.ProvenanceExtractor;
import fr.insee.vtl.prov2.SdthProgramView;
import fr.insee.vtl.testutils.InputDirectives;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Test;

/** Wave F: SDTH entity lineage + metadata on the production RDF path. */
class SdthEntityLineageTest {

  private static final ProvenanceExtractor EXTRACTOR = new ProvenanceExtractor();
  private static final String SDTH = "http://rdf-vocabulary.ddialliance.org/sdth#";
  private static final String TREVAS = "http://trevas/";

  @Test
  void identity_usesElaborationOfOnDataframeAndVariables() throws Exception {
    Program program = programFromCorpus("01-assignment");
    ProgramStep step = program.getProgramSteps().iterator().next();
    DataframeInstance produced = step.getProducedDataframe();
    assertThat(produced.getElaborationOfDataframes())
        .extracting(DataframeInstance::getLabel)
        .containsExactly("ds1");
    assertThat(produced.getWasDerivedFromDataframes()).isEmpty();

    VariableInstance id =
        produced.getHasVariableInstances().stream()
            .filter(v -> "id".equals(v.getLabel()))
            .findFirst()
            .orElseThrow();
    assertThat(id.getElaborationOfVariables())
        .extracting(VariableInstance::getLabel)
        .containsExactly("id");
  }

  @Test
  void calc_wasDerivedFromConsumedAndExpressionLeaves() throws Exception {
    Program program = programFromCorpus("03-calc");
    ProgramStep step = program.getProgramSteps().iterator().next();
    DataframeInstance produced = step.getProducedDataframe();
    assertThat(produced.getWasDerivedFromDataframes())
        .extracting(DataframeInstance::getLabel)
        .containsExactly("ds1");
    assertThat(produced.getElaborationOfDataframes()).isEmpty();

    VariableInstance varSum =
        produced.getHasVariableInstances().stream()
            .filter(v -> "var_sum".equals(v.getLabel()))
            .findFirst()
            .orElseThrow();
    assertThat(varSum.getWasDerivedFromVariables())
        .extracting(VariableInstance::getLabel)
        .containsExactlyInAnyOrder("var1", "var2");

    DataframeInstance input = step.getConsumedDataframes().iterator().next();
    assertThat(input.getWasDerivedFromFiles()).extracting(f -> f.getLabel()).containsExactly("ds1");
  }

  @Test
  void rdf_emitsHasNameHasVarInstanceLineageAndSafeIds() throws Exception {
    Program program = programFromCorpus("03-calc");
    program.setId("wave-f");
    program.setLabel("Wave F");
    Model model = RDFUtils.buildModel(program);

    Property hasName = model.createProperty(SDTH + "hasName");
    Property hasVar = model.createProperty(SDTH + "hasVarInstance");
    Property hasVarLegacy = model.createProperty(SDTH + "hasVariableInstance");
    Property hasSourceCode = model.createProperty(SDTH + "hasSourceCode");
    Property wasDerivedFrom = model.createProperty(SDTH + "wasDerivedFrom");
    Property elaborationOf = model.createProperty(SDTH + "elaborationOf");

    Resource programRes = model.createResource(TREVAS + "program/wave-f");
    assertThat(programRes.hasProperty(RDF.type, model.createResource(SDTH + "Program"))).isTrue();
    assertThat(programRes.hasProperty(hasSourceCode)).isFalse();

    assertThat(model.listStatements(null, hasVarLegacy, (Resource) null).toList()).isEmpty();
    assertThat(model.listStatements(null, hasVar, (Resource) null).toList()).isNotEmpty();
    assertThat(
            model.listStatements(null, hasName, (org.apache.jena.rdf.model.RDFNode) null).toList())
        .isNotEmpty();
    assertThat(model.listStatements(null, wasDerivedFrom, (Resource) null).toList()).isNotEmpty();

    Resource ds2 = model.createResource(TREVAS + "dataset/ds2__1");
    Resource ds1 = model.createResource(TREVAS + "dataset/ds1__0");
    Resource file = model.createResource(TREVAS + "file/file-ds1");
    assertThat(ds2.hasProperty(wasDerivedFrom, ds1)).isTrue();
    assertThat(ds1.hasProperty(wasDerivedFrom, file)).isTrue();
    assertThat(
            model
                .listResourcesWithProperty(RDF.type, model.createResource(SDTH + "FileInstance"))
                .toList())
        .isNotEmpty();

    // No raw '@' in dataset/variable/step IRIs.
    String turtle = RDFUtils.serialize(model, "TTL");
    assertThat(turtle).doesNotContain("dataset/ds2@");
    assertThat(turtle).contains("dataset/ds2__1");

    Resource step =
        model
            .listResourcesWithProperty(RDF.type, model.createResource(SDTH + "ProgramStep"))
            .nextResource();
    assertThat(step.hasProperty(hasSourceCode)).isTrue();

    // Variables must not carry hasSourceCode in RDF.
    for (Resource v :
        model
            .listResourcesWithProperty(RDF.type, model.createResource(SDTH + "VariableInstance"))
            .toList()) {
      assertThat(v.hasProperty(hasSourceCode)).isFalse();
    }

    // Identity corpus uses elaborationOf
    Model identity = RDFUtils.buildModel(programFromCorpus("01-assignment"));
    Resource ds2id = identity.createResource(TREVAS + "dataset/ds2__1");
    Resource ds1id = identity.createResource(TREVAS + "dataset/ds1__0");
    assertThat(ds2id.hasProperty(elaborationOf, ds1id)).isTrue();
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
}

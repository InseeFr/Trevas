package fr.insee.vtl.prov2.tests;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.prov.utils.PROV;
import fr.insee.vtl.prov2.ProvGraph;
import fr.insee.vtl.prov2.ProvenanceExtractor;
import fr.insee.vtl.prov2.RichRdfView;
import fr.insee.vtl.testutils.InputDirectives;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Test;

/** PR-38: ProvGraph → rich RDF ({@code wasDerivedFrom} / expression / {@code prov:used}). */
class RichRdfViewTest {

  private static final ProvenanceExtractor EXTRACTOR = new ProvenanceExtractor();
  private static final String TREVAS = "http://trevas/";
  private static final String SDTH = "http://rdf-vocabulary.ddialliance.org/sdth#";

  @Test
  void arithmetic_emitsDatasetAndVariableWasDerivedFrom() throws Exception {
    Model model = modelFromCorpus("02-arithmetic");
    Property wdf = model.createProperty(SDTH + "wasDerivedFrom");

    Resource dsSum = model.createResource(TREVAS + "dataset/ds_sum@1");
    assertThat(dsSum.hasProperty(RDF.type, model.createResource(SDTH + "DataframeInstance")))
        .isTrue();
    assertThat(dsSum.hasProperty(wdf, model.createResource(TREVAS + "dataset/ds1@0"))).isTrue();
    assertThat(dsSum.hasProperty(wdf, model.createResource(TREVAS + "dataset/ds2@0"))).isTrue();

    Resource var1 = model.createResource(TREVAS + "variable/ds_sum@1.var1");
    assertThat(var1.hasProperty(wdf, model.createResource(TREVAS + "variable/ds1@0.var1")))
        .isTrue();
    assertThat(var1.hasProperty(wdf, model.createResource(TREVAS + "variable/ds2@0.var1")))
        .isTrue();
  }

  @Test
  void calc_linksAssignedVarThroughExpressionNode() throws Exception {
    Model model = modelFromCorpus("03-calc");
    Property wdf = model.createProperty(SDTH + "wasDerivedFrom");
    Property hasSourceCode = model.createProperty(SDTH + "hasSourceCode");

    Resource expr = model.createResource(TREVAS + "expression/e1.1");
    assertThat(expr.hasProperty(RDF.type, model.createResource(TREVAS + "Expression"))).isTrue();
    assertThat(expr.hasProperty(hasSourceCode, "var1 + var2")).isTrue();
    assertThat(expr.hasProperty(wdf, model.createResource(TREVAS + "variable/ds1@0.var1")))
        .isTrue();
    assertThat(expr.hasProperty(wdf, model.createResource(TREVAS + "variable/ds1@0.var2")))
        .isTrue();

    Resource varSum = model.createResource(TREVAS + "variable/ds2@1.var_sum");
    assertThat(varSum.hasProperty(wdf, expr)).isTrue();
  }

  @Test
  void filter_conditionEdgeUsesProvUsed() throws Exception {
    Model model = modelFromCorpus("04-filter");
    Property wdf = model.createProperty(SDTH + "wasDerivedFrom");

    Resource ds2 = model.createResource(TREVAS + "dataset/ds2@1");
    Resource expr = model.createResource(TREVAS + "expression/e1.1");
    assertThat(ds2.hasProperty(wdf, model.createResource(TREVAS + "dataset/ds1@0"))).isTrue();
    assertThat(ds2.hasProperty(PROV.used, expr)).isTrue();
    assertThat(ds2.hasProperty(wdf, expr)).isFalse();
  }

  @Test
  void scalarAssign_emitsScalarAndExpression() throws Exception {
    Model model = modelFromCorpus("36-scalar-assign");
    Property wdf = model.createProperty(SDTH + "wasDerivedFrom");

    Resource x = model.createResource(TREVAS + "scalar/x@1");
    Resource e1 = model.createResource(TREVAS + "expression/e1.1");
    assertThat(x.hasProperty(RDF.type, model.createResource(TREVAS + "Scalar"))).isTrue();
    assertThat(x.hasProperty(wdf, e1)).isTrue();

    Resource y = model.createResource(TREVAS + "scalar/y@2");
    Resource e2 = model.createResource(TREVAS + "expression/e2.1");
    assertThat(y.hasProperty(wdf, e2)).isTrue();
    assertThat(e2.hasProperty(wdf, x)).isTrue();
  }

  private static Model modelFromCorpus(String folder) throws Exception {
    Path dir = corpusDir().resolve(folder);
    String script = Files.readString(dir.resolve("input.vtl"));
    ProvGraph graph = EXTRACTOR.extract(script, InputDirectives.parse(script));
    return RichRdfView.buildModel(graph);
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

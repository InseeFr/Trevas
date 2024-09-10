package fr.insee.vtl.prov;

import fr.insee.vtl.prov.utils.PropertiesLoader;
import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class RDFTest {

    static Properties conf;

    static {
        try {
            conf = PropertiesLoader.loadProperties();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static String sparqlEndpoint = conf.getProperty("sparql-endpoint-url") + conf.getProperty("trevas-provenance-repository-path");
    static String sparqlEndpointUser = conf.getProperty("sparql-endpoint-user");
    static String sparlqEndpointPassword = conf.getProperty("sparql-endpoint-password");

    String BLUEPRINT_CLASS_PATH = "docs/configuration/provenance/blueprint-class-metadata.ttl";

    String BLUEPRINT_DETAILS_PATH = "docs/configuration/provenance/blueprint-detail-metadata.ttl";

    String BLUEPRINT_LINK_PATH = "docs/configuration/provenance/blueprint-link-metadata.ttl";

    @BeforeEach
    void loadBlueprintConfig() {
        Model modelClass = RDFUtils.initModel(BLUEPRINT_CLASS_PATH);
        Model modelDetail = RDFUtils.initModel(BLUEPRINT_DETAILS_PATH);
        Model modelLink = RDFUtils.initModel(BLUEPRINT_LINK_PATH);
        Model model = modelClass.add(modelDetail).add(modelLink);
        RDFUtils.loadModelWithCredentials(model, sparqlEndpoint, sparqlEndpointUser, sparlqEndpointPassword);
    }

    @Test
    public void loadHandmadeRDF() {
        Model modelProv = RDFUtils.initModel("src/test/resources/temp-prov.ttl");
        RDFUtils.loadModelWithCredentials(modelProv, sparqlEndpoint, sparqlEndpointUser, sparlqEndpointPassword);
    }

    @Test
    public void simpleTest() {


        String script = "ds_sum := ds1 + ds2;\n" +
                "ds_mul := ds_sum * 3; \n" +
                "ds_res <- ds_mul[filter mod(var1, 2) = 0][calc var_sum := var1 + var2];";

        List<Object> obj = ProvenanceListener.parseAndListen(script, "trevas-simple-test", "Simple test from Trevas tests");
        Model model = RDFUtils.buildModel(obj);
        RDFUtils.loadModelWithCredentials(model, sparqlEndpoint, sparqlEndpointUser, sparlqEndpointPassword);
        assertThat(obj).hasSize(4);
    }

}

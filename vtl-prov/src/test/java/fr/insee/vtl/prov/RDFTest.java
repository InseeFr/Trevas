package fr.insee.vtl.prov;

import fr.insee.vtl.prov.utils.PropertiesLoader;
import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

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


    @Test
    public void loadRDF() {
        // Blueprint configuration
        Model modelClass = RDFUtils.initModel(BLUEPRINT_CLASS_PATH);
        Model modelDetail = RDFUtils.initModel(BLUEPRINT_DETAILS_PATH);
        Model modelLink = RDFUtils.initModel(BLUEPRINT_LINK_PATH);
        // Data
        Model modelProv = RDFUtils.initModel("src/test/resources/temp-prov.ttl");
        // Merge models
        Model model = modelClass.add(modelDetail).add(modelLink).add(modelProv);
        // Load model
        RDFUtils.loadModelWithCredentials(model, sparqlEndpoint, sparqlEndpointUser, sparlqEndpointPassword);
    }

}

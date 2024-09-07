package fr.insee.vtl.prov;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdfconnection.RDFConnection;

public class RDFUtils {

    private static final String SDTH_BASE_URI = "http://rdf-vocabulary.ddialliance.org/sdth";

    public static Model initModel(String baseFilePath) {
        Model model = ModelFactory.createDefaultModel();
        model.read(baseFilePath);
        return model;
    }

    public static void loadModelWithCredentials(Model model,
                                                String sparqlEndpoint,
                                                String sparqlEndpointUser,
                                                String sparqlEndpointPassword) {
        if (!sparqlEndpoint.isEmpty()) {
            RDFConnection connect = RDFConnection
                    .connectPW(sparqlEndpoint, sparqlEndpointUser, sparqlEndpointPassword);
            connect.fetchDataset();
            connect.load(model);
            connect.close();
        }
    }
}

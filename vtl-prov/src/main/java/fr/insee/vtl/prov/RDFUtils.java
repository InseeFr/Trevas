package fr.insee.vtl.prov;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFDatasetConnection;

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
            RDFConnection connection = RDFConnectionFactory
                    .connectPW(sparqlEndpoint, sparqlEndpointUser, sparqlEndpointPassword);
            connection.fetchDataset();
            connection.load(model);
            connection.close();
        }
    }
}

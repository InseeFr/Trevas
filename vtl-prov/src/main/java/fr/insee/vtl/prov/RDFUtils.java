package fr.insee.vtl.prov;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdfconnection.RDFConnection;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class RDFUtils {

    private static final String SDTH_BASE_URI = "http://rdf-vocabulary.ddialliance.org/sdth";

    public static Model initModel(String baseFilePath) {
        Model model = ModelFactory.createDefaultModel();
        model.read(baseFilePath);
        return model;
    }

    public static void loadModelWithCredentials(Model model,
                                               String sparlEndpoint,
                                               String sparlEndpointUser,
                                               String sparlEndpointPassword) {
        if (!sparlEndpoint.isEmpty()) {
            RDFConnection connect = RDFConnection
                    .connectPW(sparlEndpoint, sparlEndpointUser, sparlEndpointPassword);
            connect.fetchDataset();
            connect.load(model);
            connect.close();
        }
    }
}

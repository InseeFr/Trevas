package fr.insee.vtl.prov;

import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.utils.PROV;
import org.apache.jena.rdf.model.*;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import java.util.List;
import java.util.Set;

public class RDFUtils {

    private static final String TREVAS_BASE_URI = "http://trevas/";
    private static final String SDTH_BASE_URI = "http://rdf-vocabulary.ddialliance.org/sdth#";

    public static Model buildModel(List<Object> objects) {
        Model model = ModelFactory.createDefaultModel();
        model.setNsPrefix("prov", PROV.getURI());
        objects.forEach(o -> {
            if (o instanceof Program) {
                handleProgram(model, (Program) o);
            }
        });
        return model;
    }

    public static void handleProgram(Model model, Program program) {
        Resource SDTH_PROGRAM = model.createResource(SDTH_BASE_URI + "Program");
        String label = program.getLabel();
        Resource programURI = model.createResource(TREVAS_BASE_URI + "program/" + label);
        programURI.addProperty(RDF.type, SDTH_PROGRAM);
        programURI.addProperty(RDFS.label, label);
        String sourceCode = program.getSourceCode();
        Property SDTH_HAS_SOURCE_CODE = model.createProperty(SDTH_BASE_URI + "hasSourceCode");
        programURI.addProperty(SDTH_HAS_SOURCE_CODE, sourceCode);
        Set<String> stepIds = program.getProgramStepIds();
        stepIds.forEach(id -> {
            Property SDTH_HAS_PROGRAM_STEP = model.createProperty(SDTH_BASE_URI + "hasProgramStep");
            Resource SDTH_PROGRAM_STEP = model.createResource(SDTH_BASE_URI + "ProgramStep");
            Resource programStepURI = model.createResource(TREVAS_BASE_URI + "program-step/" + id);
            programURI.addProperty(SDTH_HAS_PROGRAM_STEP, programStepURI);
            programStepURI.addProperty(RDF.type, SDTH_PROGRAM_STEP);
            programStepURI.addProperty(RDFS.label, id);
        });
    }

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

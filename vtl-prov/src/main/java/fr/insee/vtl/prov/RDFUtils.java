package fr.insee.vtl.prov;

import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.prov.ProgramStep;
import fr.insee.vtl.prov.utils.PROV;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
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
            if (o instanceof ProgramStep) {
                handleProgramStep(model, (ProgramStep) o);
            }
        });
        return model;
    }

    public static void handleProgram(Model model, Program program) {
        Resource SDTH_PROGRAM = model.createResource(SDTH_BASE_URI + "Program");
        String id = program.getId();
        String label = program.getLabel();
        Resource programURI = model.createResource(TREVAS_BASE_URI + "program/" + id);
        programURI.addProperty(RDF.type, SDTH_PROGRAM);
        programURI.addProperty(RDFS.label, label);
        String sourceCode = program.getSourceCode();
        Property SDTH_HAS_SOURCE_CODE = model.createProperty(SDTH_BASE_URI + "hasSourceCode");
        programURI.addProperty(SDTH_HAS_SOURCE_CODE, sourceCode);
        Set<String> stepIds = program.getProgramStepIds();
        stepIds.forEach(stepId -> {
            Property SDTH_HAS_PROGRAM_STEP = model.createProperty(SDTH_BASE_URI + "hasProgramStep");
            Resource SDTH_PROGRAM_STEP = model.createResource(SDTH_BASE_URI + "ProgramStep");
            Resource programStepURI = model.createResource(TREVAS_BASE_URI + "program-step/" + stepId);
            programURI.addProperty(SDTH_HAS_PROGRAM_STEP, programStepURI);
            programStepURI.addProperty(RDF.type, SDTH_PROGRAM_STEP);
            programStepURI.addProperty(RDFS.label, "Create " + stepId + " dataset");
        });
    }

    public static void handleProgramStep(Model model, ProgramStep programStep) {
        String label = programStep.getLabel();
        Resource programStepURI = model.createResource(TREVAS_BASE_URI + "program-step/" + label);
        String sourceCode = programStep.getSourceCode();
        Property SDTH_HAS_SOURCE_CODE = model.createProperty(SDTH_BASE_URI + "hasSourceCode");
        programStepURI.addProperty(SDTH_HAS_SOURCE_CODE, sourceCode);
        Resource SDTH_DATAFRAME = model.createResource(SDTH_BASE_URI + "DataframeInstance");
        Resource dfProducesURI = model.createResource(TREVAS_BASE_URI + "dataset/" + label);
        dfProducesURI.addProperty(RDF.type, SDTH_DATAFRAME);
        dfProducesURI.addProperty(RDFS.label, label);
        Property SDTH_PRODUCES_DATAFRAME = model.createProperty(SDTH_BASE_URI + "producesDataframe");
        programStepURI.addProperty(SDTH_PRODUCES_DATAFRAME, dfProducesURI);
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

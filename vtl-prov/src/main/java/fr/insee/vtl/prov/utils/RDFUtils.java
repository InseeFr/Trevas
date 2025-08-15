package fr.insee.vtl.prov.utils;

import fr.insee.vtl.prov.prov.DataframeInstance;
import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.prov.ProgramStep;
import fr.insee.vtl.prov.prov.VariableInstance;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

public class RDFUtils {

  private static final String TREVAS_BASE_URI = "http://trevas/";
  private static final String SDTH_BASE_URI = "http://rdf-vocabulary.ddialliance.org/sdth#";

  public static Model buildModel(Program program) {
    Model model = ModelFactory.createDefaultModel();
    model.setNsPrefix("prov", PROV.getURI());
    handleProgram(model, program);
    return model;
  }

  public static void handleProgram(Model model, Program program) {
    // Create Program URI, type, label, sourceCode
    Resource SDTH_PROGRAM = model.createResource(SDTH_BASE_URI + "Program");
    String id = program.getId();
    String label = program.getLabel();
    Resource programURI = model.createResource(TREVAS_BASE_URI + "program/" + id);
    programURI.addProperty(RDF.type, SDTH_PROGRAM);
    programURI.addProperty(RDFS.label, label);
    String sourceCode = program.getSourceCode();
    Property SDTH_HAS_SOURCE_CODE = model.createProperty(SDTH_BASE_URI + "hasSourceCode");
    programURI.addProperty(SDTH_HAS_SOURCE_CODE, sourceCode);
    // Link and define ProgramSteps
    Set<ProgramStep> programSteps = program.getProgramSteps();
    Property SDTH_HAS_PROGRAM_STEP = model.createProperty(SDTH_BASE_URI + "hasProgramStep");
    programSteps.forEach(
        step -> {
          String stepId = step.getId();
          Resource programStepURI =
              model.createResource(TREVAS_BASE_URI + "program-step/" + stepId);
          programURI.addProperty(SDTH_HAS_PROGRAM_STEP, programStepURI);
          handleProgramStep(model, step);
        });
  }

  public static void handleProgramStep(Model model, ProgramStep programStep) {
    // Create ProgramStep URI, type, label, sourceCode
    String id = programStep.getId();
    Resource programStepURI = model.createResource(TREVAS_BASE_URI + "program-step/" + id);
    Resource SDTH_PROGRAM_STEP = model.createResource(SDTH_BASE_URI + "ProgramStep");
    programStepURI.addProperty(RDF.type, SDTH_PROGRAM_STEP);
    programStepURI.addProperty(RDFS.label, "Step " + programStep.getIndex());
    String sourceCode = programStep.getSourceCode();
    Property SDTH_HAS_SOURCE_CODE = model.createProperty(SDTH_BASE_URI + "hasSourceCode");
    programStepURI.addProperty(SDTH_HAS_SOURCE_CODE, sourceCode);
    // Link and define producedDF
    DataframeInstance dfProduced = programStep.getProducedDataframe();
    String dfProducedId = dfProduced.getId();
    Resource dfProducesURI = model.createResource(TREVAS_BASE_URI + "dataset/" + dfProducedId);
    Property SDTH_PRODUCES_DATAFRAME = model.createProperty(SDTH_BASE_URI + "producesDataframe");
    programStepURI.addProperty(SDTH_PRODUCES_DATAFRAME, dfProducesURI);
    handleDataframeInstance(model, dfProduced);
    // Link and define consumedDF
    Property SDTH_CONSUMES_DATAFRAME = model.createProperty(SDTH_BASE_URI + "consumesDataframe");
    programStep
        .getConsumedDataframe()
        .forEach(
            df -> {
              Resource dfConsumedURI =
                  model.createResource(TREVAS_BASE_URI + "dataset/" + df.getId());
              programStepURI.addProperty(SDTH_CONSUMES_DATAFRAME, dfConsumedURI);
              handleDataframeInstance(model, df);
            });
    // Link and define usedVariables
    Property SDTH_USED_VARIABLE = model.createProperty(SDTH_BASE_URI + "usesVariable");
    programStep
        .getUsedVariables()
        .forEach(
            v -> {
              Resource varUsedURI = model.createResource(TREVAS_BASE_URI + "variable/" + v.getId());
              programStepURI.addProperty(SDTH_USED_VARIABLE, varUsedURI);
              handleVariableInstance(model, v);
            });
    // Link and define assignedVariables
    Property SDTH_ASSIGNED_VARIABLE = model.createProperty(SDTH_BASE_URI + "assignsVariable");
    programStep
        .getAssignedVariables()
        .forEach(
            v -> {
              Resource varAssignedURI =
                  model.createResource(TREVAS_BASE_URI + "variable/" + v.getId());
              programStepURI.addProperty(SDTH_ASSIGNED_VARIABLE, varAssignedURI);
              handleVariableInstance(model, v);
            });
  }

  public static void handleDataframeInstance(Model model, DataframeInstance dfInstance) {
    // Create DataframeInstance URI, type, label
    String id = dfInstance.getId();
    Resource dfURI = model.createResource(TREVAS_BASE_URI + "dataset/" + id);
    Resource SDTH_DATAFRAME = model.createResource(SDTH_BASE_URI + "DataframeInstance");
    dfURI.addProperty(RDF.type, SDTH_DATAFRAME);
    String label = dfInstance.getLabel();
    dfURI.addProperty(RDFS.label, label);
    Property SDTH_USED_VARIABLE = model.createProperty(SDTH_BASE_URI + "hasVariableInstance");
    dfInstance
        .getHasVariableInstances()
        .forEach(
            v -> {
              Resource varAssignedURI =
                  model.createResource(TREVAS_BASE_URI + "variable/" + v.getId());
              dfURI.addProperty(SDTH_USED_VARIABLE, varAssignedURI);
              handleVariableInstance(model, v);
            });
  }

  public static void handleVariableInstance(Model model, VariableInstance varInstance) {
    // Create VariableInstance URI, type, label
    String id = varInstance.getId();
    Resource varURI = model.createResource(TREVAS_BASE_URI + "variable/" + id);
    Resource SDTH_VARIABLE = model.createResource(SDTH_BASE_URI + "VariableInstance");
    varURI.addProperty(RDF.type, SDTH_VARIABLE);
    String label = varInstance.getLabel();
    varURI.addProperty(RDFS.label, label);
    if (null != varInstance.getRole()) {
      String role = varInstance.getRole().toString();
      // TO EXTRACT
      Property hasRole = model.createProperty("http://id.making-sense.info/vtl/component/hasRole");
      varURI.addProperty(hasRole, role);
    }
    if (null != varInstance.getType()) {
      Class<?> type = varInstance.getType();
      // TO EXTRACT
      Property hasType = model.createProperty("http://id.making-sense.info/vtl/component/hasType");
      varURI.addProperty(hasType, VTLTypes.getVtlType(type));
    }
  }

  public static Model initModel(String baseFilePath) {
    Model model = ModelFactory.createDefaultModel();
    model.read(baseFilePath);
    return model;
  }

  public static void loadModelWithCredentials(
      Model model,
      String sparqlEndpoint,
      String sparqlEndpointUser,
      String sparqlEndpointPassword) {
    if (!sparqlEndpoint.isEmpty()) {
      RDFConnection connection =
          RDFConnectionFactory.connectPW(
              sparqlEndpoint, sparqlEndpointUser, sparqlEndpointPassword);
      connection.fetchDataset();
      connection.load(model);
      connection.close();
    }
  }

  public static void writeJsonLdToFile(Model model, String path) throws IOException {
    model.write(Files.newOutputStream(Path.of(path)), "JSON-LD");
  }

  public static String serialize(Model model, String format) {
    StringWriter stringWriter = new StringWriter();
    model.write(stringWriter, format);
    return stringWriter.toString();
  }
}

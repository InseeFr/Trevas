package fr.insee.vtl.prov.utils;

import fr.insee.vtl.prov.prov.DataframeInstance;
import fr.insee.vtl.prov.prov.FileInstance;
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
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

/**
 * SDTH RDF serializer for the legacy {@link Program} tree (spec 20260808_01 Phase B).
 *
 * <p>Emits process links (steps) and entity lineage ({@code wasDerivedFrom} / {@code
 * elaborationOf}). Instance URI local names replace {@code @} with {@code __} for JSON-LD safety.
 */
public class RDFUtils {

  private static final String TREVAS_BASE_URI = "http://trevas/";
  private static final String SDTH_BASE_URI = "http://rdf-vocabulary.ddialliance.org/sdth#";
  private static final String VTL_COMPONENT = "http://id.making-sense.info/vtl/component/";

  public static Model buildModel(Program program) {
    Model model = ModelFactory.createDefaultModel();
    model.setNsPrefix("sdth", SDTH_BASE_URI);
    model.setNsPrefix("prov", PROV.getURI());
    model.setNsPrefix("rdfs", RDFS.getURI());
    handleProgram(model, program);
    return model;
  }

  public static void handleProgram(Model model, Program program) {
    Resource SDTH_PROGRAM = model.createResource(SDTH_BASE_URI + "Program");
    String id = program.getId();
    String label = program.getLabel();
    Resource programURI = model.createResource(TREVAS_BASE_URI + "program/" + rdfSafeId(id));
    programURI.addProperty(RDF.type, SDTH_PROGRAM);
    programURI.addProperty(RDFS.label, label);
    // hasSourceCode is reserved for ProgramStep (Wave F / SDTH shapes).
    Set<ProgramStep> programSteps = program.getProgramSteps();
    Property SDTH_HAS_PROGRAM_STEP = model.createProperty(SDTH_BASE_URI + "hasProgramStep");
    programSteps.forEach(
        step -> {
          Resource programStepURI =
              model.createResource(TREVAS_BASE_URI + "program-step/" + rdfSafeId(step.getId()));
          programURI.addProperty(SDTH_HAS_PROGRAM_STEP, programStepURI);
          handleProgramStep(model, step);
        });
  }

  public static void handleProgramStep(Model model, ProgramStep programStep) {
    String id = programStep.getId();
    Resource programStepURI =
        model.createResource(TREVAS_BASE_URI + "program-step/" + rdfSafeId(id));
    Resource SDTH_PROGRAM_STEP = model.createResource(SDTH_BASE_URI + "ProgramStep");
    programStepURI.addProperty(RDF.type, SDTH_PROGRAM_STEP);
    programStepURI.addProperty(RDFS.label, "Step " + programStep.getIndex());
    String sourceCode = programStep.getSourceCode();
    if (sourceCode != null) {
      Property SDTH_HAS_SOURCE_CODE = model.createProperty(SDTH_BASE_URI + "hasSourceCode");
      programStepURI.addProperty(SDTH_HAS_SOURCE_CODE, sourceCode);
    }
    DataframeInstance dfProduced = programStep.getProducedDataframe();
    Resource dfProducesURI =
        model.createResource(TREVAS_BASE_URI + "dataset/" + rdfSafeId(dfProduced.getId()));
    Property SDTH_PRODUCES_DATAFRAME = model.createProperty(SDTH_BASE_URI + "producesDataframe");
    programStepURI.addProperty(SDTH_PRODUCES_DATAFRAME, dfProducesURI);
    handleDataframeInstance(model, dfProduced);
    Property SDTH_CONSUMES_DATAFRAME = model.createProperty(SDTH_BASE_URI + "consumesDataframe");
    programStep
        .getConsumedDataframes()
        .forEach(
            df -> {
              Resource dfConsumedURI =
                  model.createResource(TREVAS_BASE_URI + "dataset/" + rdfSafeId(df.getId()));
              programStepURI.addProperty(SDTH_CONSUMES_DATAFRAME, dfConsumedURI);
              handleDataframeInstance(model, df);
            });
    Property SDTH_USED_VARIABLE = model.createProperty(SDTH_BASE_URI + "usesVariable");
    programStep
        .getUsedVariables()
        .forEach(
            v -> {
              Resource varUsedURI =
                  model.createResource(TREVAS_BASE_URI + "variable/" + rdfSafeId(v.getId()));
              programStepURI.addProperty(SDTH_USED_VARIABLE, varUsedURI);
              handleVariableInstance(model, v);
            });
    Property SDTH_ASSIGNED_VARIABLE = model.createProperty(SDTH_BASE_URI + "assignsVariable");
    programStep
        .getAssignedVariables()
        .forEach(
            v -> {
              Resource varAssignedURI =
                  model.createResource(TREVAS_BASE_URI + "variable/" + rdfSafeId(v.getId()));
              programStepURI.addProperty(SDTH_ASSIGNED_VARIABLE, varAssignedURI);
              handleVariableInstance(model, v);
            });
  }

  public static void handleDataframeInstance(Model model, DataframeInstance dfInstance) {
    String id = dfInstance.getId();
    Resource dfURI = model.createResource(TREVAS_BASE_URI + "dataset/" + rdfSafeId(id));
    Resource SDTH_DATAFRAME = model.createResource(SDTH_BASE_URI + "DataframeInstance");
    dfURI.addProperty(RDF.type, SDTH_DATAFRAME);
    String label = dfInstance.getLabel();
    dfURI.addProperty(RDFS.label, label);
    Property SDTH_HAS_NAME = model.createProperty(SDTH_BASE_URI + "hasName");
    dfURI.addProperty(SDTH_HAS_NAME, label);
    Property SDTH_HAS_VAR = model.createProperty(SDTH_BASE_URI + "hasVarInstance");
    dfInstance
        .getHasVariableInstances()
        .forEach(
            v -> {
              Resource varURI =
                  model.createResource(TREVAS_BASE_URI + "variable/" + rdfSafeId(v.getId()));
              dfURI.addProperty(SDTH_HAS_VAR, varURI);
              handleVariableInstance(model, v);
            });
    Property wasDerivedFrom = model.createProperty(SDTH_BASE_URI + "wasDerivedFrom");
    Property elaborationOf = model.createProperty(SDTH_BASE_URI + "elaborationOf");
    for (DataframeInstance parent : dfInstance.getWasDerivedFromDataframes()) {
      Resource parentURI =
          model.createResource(TREVAS_BASE_URI + "dataset/" + rdfSafeId(parent.getId()));
      dfURI.addProperty(wasDerivedFrom, parentURI);
      handleDataframeInstance(model, parent);
    }
    for (FileInstance file : dfInstance.getWasDerivedFromFiles()) {
      Resource fileURI = model.createResource(TREVAS_BASE_URI + "file/" + rdfSafeId(file.getId()));
      dfURI.addProperty(wasDerivedFrom, fileURI);
      handleFileInstance(model, file);
    }
    for (DataframeInstance parent : dfInstance.getElaborationOfDataframes()) {
      Resource parentURI =
          model.createResource(TREVAS_BASE_URI + "dataset/" + rdfSafeId(parent.getId()));
      dfURI.addProperty(elaborationOf, parentURI);
      handleDataframeInstance(model, parent);
    }
  }

  public static void handleFileInstance(Model model, FileInstance fileInstance) {
    String id = fileInstance.getId();
    Resource fileURI = model.createResource(TREVAS_BASE_URI + "file/" + rdfSafeId(id));
    Resource SDTH_FILE = model.createResource(SDTH_BASE_URI + "FileInstance");
    fileURI.addProperty(RDF.type, SDTH_FILE);
    String label = fileInstance.getLabel();
    fileURI.addProperty(RDFS.label, label);
    Property SDTH_HAS_NAME = model.createProperty(SDTH_BASE_URI + "hasName");
    fileURI.addProperty(SDTH_HAS_NAME, label);
    Property SDTH_HAS_VAR = model.createProperty(SDTH_BASE_URI + "hasVarInstance");
    fileInstance
        .getHasVariableInstances()
        .forEach(
            v -> {
              Resource varURI =
                  model.createResource(TREVAS_BASE_URI + "variable/" + rdfSafeId(v.getId()));
              fileURI.addProperty(SDTH_HAS_VAR, varURI);
              handleVariableInstance(model, v);
            });
  }

  public static void handleVariableInstance(Model model, VariableInstance varInstance) {
    String id = varInstance.getId();
    Resource varURI = model.createResource(TREVAS_BASE_URI + "variable/" + rdfSafeId(id));
    Resource SDTH_VARIABLE = model.createResource(SDTH_BASE_URI + "VariableInstance");
    varURI.addProperty(RDF.type, SDTH_VARIABLE);
    String label = varInstance.getLabel();
    varURI.addProperty(RDFS.label, label);
    Property SDTH_HAS_NAME = model.createProperty(SDTH_BASE_URI + "hasName");
    varURI.addProperty(SDTH_HAS_NAME, label);
    if (null != varInstance.getRole()) {
      String role = varInstance.getRole().toString();
      Property hasRole = model.createProperty(VTL_COMPONENT + "hasRole");
      varURI.addProperty(hasRole, role);
    }
    if (null != varInstance.getType()) {
      Class<?> type = varInstance.getType();
      Property hasType = model.createProperty(VTL_COMPONENT + "hasType");
      varURI.addProperty(hasType, VTLTypes.getVtlType(type));
    }
    // Do not emit hasSourceCode on variables (Wave F).
    Property wasDerivedFrom = model.createProperty(SDTH_BASE_URI + "wasDerivedFrom");
    Property elaborationOf = model.createProperty(SDTH_BASE_URI + "elaborationOf");
    for (VariableInstance parent : varInstance.getWasDerivedFromVariables()) {
      Resource parentURI =
          model.createResource(TREVAS_BASE_URI + "variable/" + rdfSafeId(parent.getId()));
      varURI.addProperty(wasDerivedFrom, parentURI);
      handleVariableInstance(model, parent);
    }
    for (VariableInstance parent : varInstance.getElaborationOfVariables()) {
      Resource parentURI =
          model.createResource(TREVAS_BASE_URI + "variable/" + rdfSafeId(parent.getId()));
      varURI.addProperty(elaborationOf, parentURI);
      handleVariableInstance(model, parent);
    }
  }

  /** Encode IR version markers so JSON-LD does not treat {@code @} as a keyword escape. */
  public static String rdfSafeId(String id) {
    if (id == null) {
      return "";
    }
    return id.replace("@", "__").replace("#", "_");
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
      try (RDFConnection connection =
          RDFConnection.connectPW(sparqlEndpoint, sparqlEndpointUser, sparqlEndpointPassword)) {
        connection.fetchDataset();
        connection.load(model);
      }
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

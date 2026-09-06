package fr.insee.vtl.prov;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.utils.VTLTypes;
import fr.insee.vtl.prov2.ProvGraph;
import fr.insee.vtl.prov2.ProvenanceExtractor;
import fr.insee.vtl.prov2.SdthProgramView;
import fr.insee.vtl.testutils.InputDataset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * Public provenance entry: eval on the caller’s engine (failure semantics), then extract a {@link
 * ProvGraph} and project it to a legacy {@link Program} for {@link
 * fr.insee.vtl.prov.utils.RDFUtils}.
 */
public final class Provenance {

  private static final ProvenanceExtractor EXTRACTOR = new ProvenanceExtractor();

  private Provenance() {}

  /**
   * Runs {@code script} on {@code engine}, then builds an SDTH {@link Program} from the new
   * provenance IR.
   *
   * @param engine engine whose bindings supply input datasets (and which executes the script)
   * @param script VTL source
   * @param id program id (URI segment)
   * @param label program label
   */
  public static Program run(ScriptEngine engine, String script, String id, String label)
      throws ScriptException {
    List<InputDataset> inputs = inputsFrom(engine.getContext());
    engine.eval(script);
    ProvGraph graph = EXTRACTOR.extract(script, inputs);
    return SdthProgramView.toProgram(graph, id, label, script);
  }

  /** Snapshot {@link Dataset} bindings as {@link InputDataset} structures (no rows). */
  static List<InputDataset> inputsFrom(ScriptContext context) {
    List<InputDataset> inputs = new ArrayList<>();
    for (int scope : List.of(ScriptContext.ENGINE_SCOPE, ScriptContext.GLOBAL_SCOPE)) {
      Bindings bindings = context.getBindings(scope);
      if (bindings == null) {
        continue;
      }
      for (Map.Entry<String, Object> entry : bindings.entrySet()) {
        if (entry.getValue() instanceof Dataset dataset) {
          inputs.add(fromDataset(entry.getKey(), dataset));
        }
      }
    }
    return inputs;
  }

  private static InputDataset fromDataset(String name, Dataset dataset) {
    List<InputDataset.Column> columns = new ArrayList<>();
    for (Component component : dataset.getDataStructure().componentsInOrder()) {
      columns.add(
          new InputDataset.Column(
              component.getName(),
              VTLTypes.getVtlType(component.getType()),
              component.getRole().name(),
              Map.of()));
    }
    return new InputDataset(name, columns);
  }
}

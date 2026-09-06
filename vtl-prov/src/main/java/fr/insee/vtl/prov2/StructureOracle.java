package fr.insee.vtl.prov2;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured.DataStructure;
import fr.insee.vtl.testutils.InputDataset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Run-once structure oracle (spec 20260728_01): eval the script, then read {@link DataStructure}
 * from named bindings.
 *
 * <p>Eval may fail when the engine lacks an operator that provenance already covers ({@code
 * intersect}, analytic windows, …). In that case {@link #evalSucceeded()} is {@code false}, input
 * bindings remain available, and {@link ProvenanceVisitor} derives missing output structures from
 * {@link PendingOp} via {@link StructureDeriver}. Anonymous intermediates are always derived
 * (never engine-bound).
 *
 * <p>Structure rule for a named assignment LHS: if {@link #hasDataset(String)} then use the
 * engine binding; otherwise derive from the pending op. Do not mix column types from both sources
 * for one dataset.
 */
final class StructureOracle {

  private final ScriptContext context;
  private final boolean evalSucceeded;

  private StructureOracle(ScriptContext context, boolean evalSucceeded) {
    this.context = context;
    this.evalSucceeded = evalSucceeded;
  }

  static StructureOracle run(String script, List<InputDataset> inputs) {
    ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
    if (engine == null) {
      throw new IllegalStateException("no VTL script engine on the classpath");
    }
    ScriptContext context = engine.getContext();
    for (InputDataset input : inputs) {
      context.setAttribute(input.name(), toDataset(input), ScriptContext.ENGINE_SCOPE);
    }
    boolean succeeded = true;
    try {
      engine.eval(script);
    } catch (ScriptException e) {
      // Engine wraps many failures; inputs stay bound for derivation.
      succeeded = false;
    } catch (RuntimeException e) {
      // Bare UOE / NPE from unimplemented or half-wired ops (customPivot, …).
      succeeded = false;
    }
    return new StructureOracle(context, succeeded);
  }

  /** {@code true} when {@code engine.eval} completed without throwing. */
  boolean evalSucceeded() {
    return evalSucceeded;
  }

  boolean hasDataset(String name) {
    return context.getAttribute(name) instanceof Dataset;
  }

  DataStructure requireDataset(String name) {
    Object value = context.getAttribute(name);
    if (value instanceof Dataset dataset) {
      return dataset.getDataStructure();
    }
    throw new UnsupportedOperationException("unsupported: scalar");
  }

  private static Dataset toDataset(InputDataset input) {
    Map<String, Class<?>> types = new LinkedHashMap<>();
    Map<String, Dataset.Role> roles = new LinkedHashMap<>();
    for (InputDataset.Column column : input.columns()) {
      types.put(column.name(), VtlJavaTypes.javaType(column.type()));
      roles.put(column.name(), Dataset.Role.valueOf(column.role()));
    }
    if (input.rows().isEmpty()) {
      return new InMemoryDataset(List.of(), types, roles);
    }
    DataStructure structure = new DataStructure(types, roles);
    List<List<Object>> data = new ArrayList<>();
    for (List<String> row : input.rows()) {
      List<Object> values = new ArrayList<>(row.size());
      for (int i = 0; i < row.size(); i++) {
        values.add(VtlJavaTypes.cellValue(row.get(i), input.columns().get(i).type()));
      }
      data.add(values);
    }
    return new InMemoryDataset(data, structure);
  }
}

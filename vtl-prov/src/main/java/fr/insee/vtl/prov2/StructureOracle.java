package fr.insee.vtl.prov2;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Run-once structure oracle (spec 20260728_01): eval the script, then read {@link DataStructure}
 * from named bindings. Anonymous intermediates are out of scope until a later PR.
 */
final class StructureOracle {

  private final ScriptContext context;

  private StructureOracle(ScriptContext context) {
    this.context = context;
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
    try {
      engine.eval(script);
    } catch (ScriptException e) {
      throw new IllegalStateException("structure oracle failed to eval script", e);
    }
    return new StructureOracle(context);
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
      types.put(column.name(), javaType(column.type()));
      roles.put(column.name(), Dataset.Role.valueOf(column.role()));
    }
    return new InMemoryDataset(List.of(), types, roles);
  }

  private static Class<?> javaType(String vtlType) {
    return switch (vtlType) {
      case "STRING" -> String.class;
      case "INTEGER" -> Long.class;
      case "NUMBER" -> Double.class;
      case "BOOLEAN" -> Boolean.class;
      default -> throw new UnsupportedOperationException("unsupported: type " + vtlType);
    };
  }
}

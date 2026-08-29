package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.visitors.DAGBuildingVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.script.Bindings;

/** Snapshots outer bindings for UDO free variables at define time. */
final class UdoClosureBindings {

  private UdoClosureBindings() {}

  static Map<String, Object> capture(VtlParser.DefOperatorContext ctx, Bindings bindings) {
    Set<String> freeVars = DAGBuildingVisitor.udoFreeVariableNames(ctx);
    Map<String, Object> snapshot = new HashMap<>();
    for (String name : freeVars) {
      if (bindings.containsKey(name)) {
        snapshot.put(name, bindings.get(name));
      }
    }
    return Map.copyOf(snapshot);
  }
}

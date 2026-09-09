package fr.insee.vtl.prov.extract;

import fr.insee.vtl.model.Structured.DataStructure;
import java.util.Map;
import java.util.function.Function;

/** Shared structure lookup for deriver / linker / visitor. */
final class Structures {

  private Structures() {}

  static DataStructure require(Function<String, DataStructure> structures, String id) {
    DataStructure structure = structures.apply(id);
    if (structure == null) {
      throw new IllegalStateException("unknown structure for " + id);
    }
    return structure;
  }

  static DataStructure require(Map<String, DataStructure> structures, String id) {
    DataStructure structure = structures.get(id);
    if (structure == null) {
      throw new IllegalStateException("unknown structure for " + id);
    }
    return structure;
  }
}

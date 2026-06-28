package fr.insee.vtl.engine.semantics.join;

import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;

/** Resolves physical join column names from bare output names. */
public final class JoinColumnNames {

  private JoinColumnNames() {}

  /** Prefer {@code alias#name} over bare {@code name} when both exist after a join rename. */
  public static String resolveSourceColumn(DataStructure source, String bareName) {
    String bare = null;
    String aliased = null;
    for (Component component : source.componentsInOrder()) {
      String name = component.getName();
      if (!stripJoinAlias(name).equals(bareName)) {
        continue;
      }
      if (name.contains("#")) {
        aliased = name;
      } else if (name.equals(bareName)) {
        bare = name;
      }
    }
    if (aliased != null) {
      return aliased;
    }
    if (bare != null) {
      return bare;
    }
    return bareName;
  }

  public static String stripJoinAlias(String columnName) {
    return columnName.substring(columnName.lastIndexOf('#') + 1);
  }
}

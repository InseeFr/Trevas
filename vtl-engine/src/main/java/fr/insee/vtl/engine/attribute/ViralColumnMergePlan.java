package fr.insee.vtl.engine.attribute;

import static fr.insee.vtl.engine.join.JoinProjection.stripJoinAlias;

import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.ArrayList;
import java.util.List;

/** Locates physical columns that contribute to one bare viral attribute after a join. */
public final class ViralColumnMergePlan {

  private ViralColumnMergePlan() {}

  /** Columns in {@code structure} that map to {@code bareName} and are viral. */
  public static List<Component> viralSources(DataStructure structure, String bareName) {
    List<Component> matches = new ArrayList<>();
    for (Component component : structure.componentsInOrder()) {
      if (stripJoinAlias(component.getName()).equals(bareName) && component.isViralAttribute()) {
        matches.add(component);
      }
    }
    return matches;
  }

  public static boolean needsValueMerge(DataStructure structure, String bareName) {
    return viralSources(structure, bareName).size() > 1;
  }
}

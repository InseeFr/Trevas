package fr.insee.vtl.engine.join;

import static fr.insee.vtl.model.Structured.Component;

import fr.insee.vtl.model.Structured.DataStructure;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Builds the output {@link DataStructure} of a multi-operand join (identifiers + operands). */
public final class JoinStructureBuilder {

  private JoinStructureBuilder() {}

  public static DataStructure build(List<Component> joinKeys, List<DataStructure> operands) {
    List<Component> components = new ArrayList<>(joinKeys);
    for (DataStructure operand : operands) {
      appendJoinOperands(components, joinKeys, operand);
    }
    return new DataStructure(components);
  }

  public static DataStructure build(List<Component> joinKeys, DataStructure... operands) {
    return build(joinKeys, List.of(operands));
  }

  /** Appends non-key components in VTL order: identifiers, measures, viral, other attributes. */
  private static void appendJoinOperands(
      List<Component> target, List<Component> joinKeys, DataStructure structure) {
    Set<String> joinKeyNames =
        joinKeys.stream().map(Component::getName).collect(Collectors.toSet());
    Set<String> present =
        target.stream()
            .map(Component::getName)
            .collect(Collectors.toCollection(LinkedHashSet::new));

    List<Component> ids = new ArrayList<>();
    List<Component> measures = new ArrayList<>();
    List<Component> viral = new ArrayList<>();
    List<Component> attributes = new ArrayList<>();

    for (Component component : structure.componentsInOrder()) {
      String name = component.getName();
      if (joinKeyNames.contains(name) || present.contains(name)) {
        continue;
      }
      if (component.isIdentifier()) {
        ids.add(component);
      } else if (component.isMeasure()) {
        measures.add(component);
      } else if (component.isViralAttribute()) {
        viral.add(component);
      } else if (component.isAttribute()) {
        attributes.add(component);
      }
    }
    for (List<Component> batch : List.of(ids, measures, viral, attributes)) {
      for (Component component : batch) {
        if (present.add(component.getName())) {
          target.add(component);
        }
      }
    }
  }
}

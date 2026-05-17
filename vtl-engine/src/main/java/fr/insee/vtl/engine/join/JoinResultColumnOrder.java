package fr.insee.vtl.engine.join;

import static fr.insee.vtl.model.Structured.Component;

import fr.insee.vtl.model.Structured.DataStructure;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Canonical column order for a join virtual dataset (engine + Spark tests). */
public final class JoinResultColumnOrder {

  private JoinResultColumnOrder() {}

  public static boolean hasAliasedColumn(DataStructure structure) {
    return structure.componentsInOrder().stream().anyMatch(c -> c.getName().contains("#"));
  }

  public static List<String> compute(
      DataStructure joinStructure,
      List<Component> joinKeys,
      List<DataStructure> operandsInJoinOrder) {

    Set<String> joinKeyNames =
        joinKeys.stream().map(Component::getName).collect(Collectors.toSet());
    Set<String> wanted =
        joinStructure.values().stream()
            .map(c -> JoinProjection.stripJoinAlias(c.getName()))
            .collect(Collectors.toSet());

    List<String> nonKeyIds = new ArrayList<>();
    for (Component component : joinStructure.getIdentifiers()) {
      String bare = JoinProjection.stripJoinAlias(component.getName());
      if (wanted.contains(bare) && !joinKeyNames.contains(bare) && !nonKeyIds.contains(bare)) {
        nonKeyIds.add(bare);
      }
    }

    List<String> measures = new ArrayList<>();
    for (Component component : joinStructure.getMeasures()) {
      String bare = JoinProjection.stripJoinAlias(component.getName());
      if (wanted.contains(bare) && !measures.contains(bare)) {
        measures.add(bare);
      }
    }

    List<String> ordered;
    if (!nonKeyIds.isEmpty()) {
      ordered = partialUsingOrder(joinKeys, nonKeyIds, measures);
    } else if (operandsInJoinOrder.size() > 2 && measures.size() > 2) {
      // Spark-style 3-way join (name, age, weight, age2, …), not full_join on the same id+m1.
      ordered = multiOperandNaturalJoinOrder(joinKeys, operandsInJoinOrder);
    } else {
      // Keys then measures: {@code id1, id2, m1, m2} or collapsed {@code id, m1}.
      ordered = keysThenMeasures(joinKeys, measures);
    }
    return appendRemainingColumns(ordered, joinStructure, wanted);
  }

  private static List<String> appendRemainingColumns(
      List<String> ordered, DataStructure structure, Set<String> wanted) {
    List<String> result = new ArrayList<>(ordered);
    for (Component component : structure.getViralAttributes()) {
      String name = JoinProjection.stripJoinAlias(component.getName());
      if (wanted.contains(name) && !result.contains(name)) {
        result.add(name);
      }
    }
    for (Component component : structure.getAttributes()) {
      String name = JoinProjection.stripJoinAlias(component.getName());
      if (wanted.contains(name) && !result.contains(name)) {
        result.add(name);
      }
    }
    for (String name : wanted) {
      if (!result.contains(name)) {
        result.add(name);
      }
    }
    return result;
  }

  /** {@code using} on a strict subset of identifiers (duplicate renamed with {@code #}). */
  private static List<String> partialUsingOrder(
      List<Component> joinKeys, List<String> nonKeyIds, List<String> measures) {
    List<String> keys = joinKeys.stream().map(Component::getName).toList();
    List<String> ordered = new ArrayList<>();
    if (!keys.isEmpty()) {
      ordered.add(keys.get(0));
    }
    if (!measures.isEmpty()) {
      ordered.add(measures.get(0));
    }
    ordered.addAll(nonKeyIds);
    for (int i = 1; i < measures.size(); i++) {
      ordered.add(measures.get(i));
    }
    for (int i = 1; i < keys.size(); i++) {
      String key = keys.get(i);
      if (!ordered.contains(key)) {
        ordered.add(key);
      }
    }
    return ordered;
  }

  /**
   * Three or more operands, natural join on identifiers only (Spark {@code JoinTest} order): join
   * keys, first reversed measure of operand 1, then non-keys of following operands from last to
   * second, then remaining measures of operand 1, then first measure of operand 2 when 3-way.
   */
  private static List<String> multiOperandNaturalJoinOrder(
      List<Component> joinKeys, List<DataStructure> operands) {
    Set<String> joinKeyNames =
        joinKeys.stream().map(Component::getName).collect(Collectors.toSet());
    List<String> result = joinKeys.stream().map(Component::getName).collect(Collectors.toList());

    List<String> firstMeasures = measureNames(operands.get(0), joinKeyNames);
    List<String> firstMeasuresRev = new ArrayList<>(firstMeasures);
    Collections.reverse(firstMeasuresRev);
    if (!firstMeasuresRev.isEmpty()) {
      result.add(firstMeasuresRev.get(0));
    }

    for (int i = operands.size() - 1; i >= 1; i--) {
      List<String> nonKeyNames = nonKeyNamesInOrder(operands.get(i), joinKeyNames);
      if (i == 1 && operands.size() > 2) {
        List<String> operandMeasures =
            measureNames(operands.get(i), joinKeyNames).stream()
                .filter(nonKeyNames::contains)
                .toList();
        if (operandMeasures.size() > 1) {
          result.add(operandMeasures.get(1));
        }
      } else {
        result.addAll(nonKeyNames);
      }
    }

    if (firstMeasuresRev.size() > 1) {
      result.addAll(firstMeasuresRev.subList(1, firstMeasuresRev.size()));
    }

    if (operands.size() > 2) {
      List<String> secondOperandMeasures = measureNames(operands.get(1), joinKeyNames);
      if (!secondOperandMeasures.isEmpty()) {
        result.add(secondOperandMeasures.get(0));
      }
    }

    return result;
  }

  private static List<String> keysThenMeasures(List<Component> joinKeys, List<String> measures) {
    List<String> ordered = new ArrayList<>();
    joinKeys.stream().map(Component::getName).forEach(ordered::add);
    ordered.addAll(measures);
    return ordered;
  }

  private static List<String> measureNames(DataStructure operand, Set<String> joinKeyNames) {
    return operand.componentsInOrder().stream()
        .filter(c -> !joinKeyNames.contains(c.getName()))
        .filter(Component::isMeasure)
        .map(Component::getName)
        .toList();
  }

  private static List<String> nonKeyNamesInOrder(DataStructure operand, Set<String> joinKeyNames) {
    return operand.componentsInOrder().stream()
        .filter(c -> !joinKeyNames.contains(c.getName()))
        .map(Component::getName)
        .toList();
  }
}

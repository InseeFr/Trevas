package fr.insee.vtl.engine.attribute;

import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Target structure and keys for unary / binary viral reattach (engine-agnostic). */
public final class ViralAttributeReattachPlan {

  private final DataStructure targetStructure;
  private final List<String> identifierNames;
  private final Set<String> viralNames;

  private ViralAttributeReattachPlan(
      DataStructure targetStructure, List<String> identifierNames, Set<String> viralNames) {
    this.targetStructure = targetStructure;
    this.identifierNames = identifierNames;
    this.viralNames = viralNames;
  }

  public static ViralAttributeReattachPlan unary(
      DatasetExpression source, Map<String, Class<?>> outputMeasuresByName) {
    DataStructure sourceStructure = source.getDataStructure();
    DataStructure target =
        AttributePropagation.unaryStructure(sourceStructure, outputMeasuresByName);
    List<String> ids = sourceStructure.getIdentifiers().stream().map(Component::getName).toList();
    return new ViralAttributeReattachPlan(
        target, ids, AttributePropagation.viralAttributeNames(sourceStructure));
  }

  public static ViralAttributeReattachPlan binary(
      List<DatasetExpression> sources, Map<String, Class<?>> outputMeasuresByName) {
    DataStructure[] operandStructures =
        sources.stream().map(DatasetExpression::getDataStructure).toArray(DataStructure[]::new);
    DataStructure mergedOperands = AttributePropagation.mergeStructure(operandStructures);
    DataStructure target =
        AttributePropagation.unaryStructure(mergedOperands, outputMeasuresByName);
    List<String> ids = mergedOperands.getIdentifiers().stream().map(Component::getName).toList();
    return new ViralAttributeReattachPlan(
        target, ids, AttributePropagation.viralAttributeNames(mergedOperands));
  }

  public DataStructure targetStructure() {
    return targetStructure;
  }

  public List<String> identifierNames() {
    return identifierNames;
  }

  public Set<String> viralNames() {
    return viralNames;
  }

  public boolean hasVirals() {
    return !viralNames.isEmpty();
  }

  public Class<?> viralType(String viralName) {
    return targetStructure.get(viralName).getType();
  }
}

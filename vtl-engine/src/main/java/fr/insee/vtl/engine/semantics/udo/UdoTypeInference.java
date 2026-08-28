package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.visitors.DAGBuildingVisitor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.DataPointRuleset;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Static type inference for UDO bodies when {@code returns} is omitted. */
final class UdoTypeInference {

  private UdoTypeInference() {}

  static Class<?> inferBodyType(UdoDefinition udo) {
    Set<String> formalNames =
        udo.getParameters().stream().map(UdoParameter::getName).collect(Collectors.toSet());
    Map<String, Object> scope = new HashMap<>(udo.getClosureBindings());
    for (UdoParameter formal : udo.getParameters()) {
      scope.putIfAbsent(formal.getName(), new UdoBindingPlaceholder(placeholderType(formal)));
    }
    for (String freeName : DAGBuildingVisitor.udoFreeVariableNames(udo.getBody(), formalNames)) {
      scope.putIfAbsent(freeName, new UdoBindingPlaceholder(Object.class));
    }
    var engine = udo.getEngine();
    ExpressionVisitor visitor =
        new ExpressionVisitor(scope, engine.getProcessingEngine(), engine);
    return visitor.visit(udo.getBody()).getType();
  }

  private static Class<?> placeholderType(UdoParameter formal) {
    if (formal.isComponentParam()) {
      return Structured.Component.class;
    }
    if (formal.isRulesetParam()) {
      return formal.getRulesetKind() == UdoRulesetKind.HIERARCHICAL
          ? fr.insee.vtl.model.HierarchicalRuleset.class
          : DataPointRuleset.class;
    }
    if (formal.getDatasetSignature() != null || formal.getType() == Dataset.class) {
      return Dataset.class;
    }
    return formal.getType();
  }
}

package fr.insee.vtl.engine.membership;

import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.Structured;
import java.util.Map;

/** Applies membership ({@code #}) using the processing engine. */
public final class MembershipOperations {

  private MembershipOperations() {}

  public static DatasetExpression execute(
      ProcessingEngine engine, DatasetExpression dataset, String memberComponentName) {
    MembershipPlan plan = MembershipPlan.of(dataset.getDataStructure(), memberComponentName);
    if (!plan.promoteToMeasure()) {
      return engine.executeProject(dataset, plan.projectColumns());
    }
    Structured.Component member = dataset.getDataStructure().get(plan.memberComponentName());
    ComponentExpression memberRef = new ComponentExpression(member, dataset);
    DatasetExpression withMeasure =
        engine.executeCalc(
            dataset,
            Map.of(plan.derivedMeasureName(), memberRef),
            Map.of(plan.derivedMeasureName(), Dataset.Role.MEASURE),
            Map.of());
    return engine.executeProject(withMeasure, plan.projectColumns());
  }
}

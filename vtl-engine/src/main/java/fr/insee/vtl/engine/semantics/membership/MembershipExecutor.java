package fr.insee.vtl.engine.semantics.membership;

import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.engine.semantics.DatasetResults;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.Structured;
import java.util.Map;

/** Applies membership ({@code #}) using the processing engine. */
public final class MembershipExecutor {

  private MembershipExecutor() {}

  public static DatasetExpression execute(
      ProcessingEngine engine, DatasetExpression dataset, String memberComponentName) {
    MembershipPlan plan = MembershipPlan.of(dataset.getDataStructure(), memberComponentName);
    Structured.DataStructure resultStructure =
        MembershipStructureBuilder.build(dataset.getDataStructure(), plan);

    DatasetExpression mechanical;
    if (!plan.promoteToMeasure()) {
      mechanical = engine.executeProject(dataset, plan.projectColumns());
    } else {
      Structured.Component member = dataset.getDataStructure().get(plan.memberComponentName());
      ComponentExpression memberRef = new ComponentExpression(member, dataset);
      mechanical =
          engine.executeProject(
              engine.executeCalc(
                  dataset,
                  Map.of(plan.derivedMeasureName(), memberRef),
                  Map.of(plan.derivedMeasureName(), Dataset.Role.MEASURE),
                  Map.of()),
              plan.projectColumns());
    }
    return DatasetResults.withStructure(mechanical, resultStructure);
  }
}

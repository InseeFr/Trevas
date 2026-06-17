package fr.insee.vtl.engine.aggregation;

import fr.insee.vtl.engine.DatasetResults;
import fr.insee.vtl.engine.visitors.GroupAllVisitor;
import fr.insee.vtl.engine.visitors.GroupByVisitor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Resolves grouping clauses and applies group-all calc when needed. */
public final class GroupingResolver {

  private GroupingResolver() {}

  /**
   * @param groupingHost parse tree node that contains an optional {@link
   *     VtlParser.GroupingClauseContext} child (e.g. {@link VtlParser.AggrClauseContext} or {@link
   *     VtlParser.AggrDatasetContext}).
   */
  public static GroupingPlan resolve(
      DatasetExpression dataset,
      VtlParser.GroupingClauseContext groupingClause,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine processingEngine) {
    GroupingPlan.Builder plan = GroupingPlan.builder(dataset);
    List<String> groupBy = new ArrayList<>();

    if (groupingClause != null) {
      ResolvableExpression groupAll = new GroupAllVisitor(expressionVisitor).visit(groupingClause);
      if (groupAll != null) {
        DatasetExpression withGroupAll =
            processingEngine.executeCalc(
                plan.getDataset(),
                Map.of("time", groupAll),
                Map.of("time", Dataset.Role.IDENTIFIER),
                Map.of());
        plan.withDataset(withGroupAll);
        groupBy.add("time");
      }
      groupBy.addAll(
          new GroupByVisitor(plan.getDataset().getDataStructure()).visit(groupingClause));
    }

    DatasetExpression groupedDataset =
        DatasetResults.withStructure(
            plan.getDataset(), groupByStructure(plan.getDataset().getDataStructure(), groupBy));

    return GroupingPlan.builder(groupedDataset).addGroupByKeys(groupBy).build();
  }

  private static Structured.DataStructure groupByStructure(
      Structured.DataStructure input, List<String> groupBy) {
    List<Structured.Component> components =
        input.componentsInOrder().stream()
            .map(
                component ->
                    groupBy.contains(component.getName())
                        ? new Structured.Component(
                            component.getName(), component.getType(), Dataset.Role.IDENTIFIER)
                        : new Structured.Component(component))
            .toList();
    return new Structured.DataStructure(components);
  }
}

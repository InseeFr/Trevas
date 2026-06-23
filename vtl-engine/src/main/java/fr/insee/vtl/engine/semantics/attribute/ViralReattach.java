package fr.insee.vtl.engine.semantics.attribute;

import fr.insee.vtl.engine.semantics.join.JoinFinalization;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Restores viral columns after dataset functions using {@link ProcessingEngine} primitives. */
public final class ViralReattach {

  private ViralReattach() {}

  public static DatasetExpression unary(
      ProcessingEngine engine,
      DatasetExpression source,
      DatasetExpression transformed,
      Map<String, Class<?>> outputMeasuresByName) {
    ViralAttributeReattachPlan plan =
        ViralAttributeReattachPlan.unary(source, outputMeasuresByName);
    if (!plan.hasVirals()) {
      return withStructure(transformed, plan.targetStructure());
    }
    List<String> sideCols = new ArrayList<>(plan.identifierNames());
    sideCols.addAll(plan.viralNames());
    return joinSide(engine, transformed, engine.executeProject(source, sideCols), plan);
  }

  public static DatasetExpression binary(
      ProcessingEngine engine,
      List<DatasetExpression> sources,
      DatasetExpression transformed,
      Map<String, Class<?>> outputMeasuresByName) {
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("at least one source dataset is required");
    }
    if (sources.size() == 1) {
      return unary(engine, sources.get(0), transformed, outputMeasuresByName);
    }
    ViralAttributeReattachPlan plan =
        ViralAttributeReattachPlan.binary(sources, outputMeasuresByName);
    if (!plan.hasVirals()) {
      return unary(engine, sources.get(0), transformed, outputMeasuresByName);
    }
    LinkedHashMap<String, DatasetExpression> operands = new LinkedHashMap<>();
    operands.put("base", transformed);
    int index = 0;
    for (DatasetExpression source : sources) {
      List<String> sideCols = new ArrayList<>(plan.identifierNames());
      sideCols.addAll(plan.viralNames());
      operands.put("s" + index++, engine.executeProject(source, sideCols));
    }
    List<Component> keys =
        plan.identifierNames().stream().map(plan.targetStructure()::get).toList();
    DatasetExpression joined = engine.executeInnerJoin(operands, keys);
    List<String> out =
        plan.targetStructure().componentsInOrder().stream().map(Component::getName).toList();
    return JoinFinalization.apply(engine, joined, out);
  }

  private static DatasetExpression joinSide(
      ProcessingEngine engine,
      DatasetExpression base,
      DatasetExpression side,
      ViralAttributeReattachPlan plan) {
    LinkedHashMap<String, DatasetExpression> operands = new LinkedHashMap<>();
    operands.put("base", base);
    operands.put("src", side);
    List<Component> keys =
        plan.identifierNames().stream().map(plan.targetStructure()::get).toList();
    DatasetExpression joined = engine.executeInnerJoin(operands, keys);
    List<String> out =
        plan.targetStructure().componentsInOrder().stream().map(Component::getName).toList();
    return JoinFinalization.apply(engine, joined, out);
  }

  private static DatasetExpression withStructure(
      DatasetExpression expression, DataStructure structure) {
    return new DatasetExpression(expression) {
      @Override
      public DataStructure getDataStructure() {
        return structure;
      }

      @Override
      public Dataset resolve(Map<String, Object> context) {
        return expression.resolve(context);
      }
    };
  }
}

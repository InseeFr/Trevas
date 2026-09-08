package fr.insee.vtl.engine.semantics.comparison;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.DefaultMeasureNames.BOOL_VAR;
import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * VTL {@code exists_in(op1, op2, retain)}: identifiers of {@code op1} plus {@code bool_var},
 * filtered by retain ({@code all} / {@code true} / {@code false}).
 */
public final class ExistsInExecutor {

  private enum Retain {
    ALL,
    TRUE,
    FALSE
  }

  private ExistsInExecutor() {}

  public static DatasetExpression execute(
      VtlParser.ExistInAtomContext ctx,
      ResolvableExpression leftExpr,
      ResolvableExpression rightExpr,
      ProcessingEngine engine) {
    Positioned position = fromContext(ctx);
    DatasetExpression left =
        (DatasetExpression) assertTypeExpression(leftExpr, Dataset.class, ctx.left);
    DatasetExpression right =
        (DatasetExpression) assertTypeExpression(rightExpr, Dataset.class, ctx.right);
    Retain retain = parseRetain(ctx.retainType(), position);

    List<String> leftIds = identifierNames(left);
    List<String> rightIds = identifierNames(right);
    if (leftIds.isEmpty() || rightIds.isEmpty()) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "exists_in requires identifier components on both operands", position));
    }
    if (!containsAll(leftIds, rightIds) && !containsAll(rightIds, leftIds)) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "exists_in: one operand must contain all identifier components of the other",
              position));
    }

    List<String> commonIds = commonIdentifierNames(leftIds, rightIds);
    DatasetExpression leftKeys = engine.executeProject(left, leftIds);
    DatasetExpression rightKeys = engine.executeProject(right, commonIds);

    DatasetExpression matched =
        withBoolVar(
            engine.executeIntersect(List.of(leftKeys, rightKeys), commonIds),
            true,
            position,
            engine);
    DatasetExpression unmatched =
        withBoolVar(engine.executeSetDiff(leftKeys, rightKeys, commonIds), false, position, engine);

    return switch (retain) {
      case TRUE -> matched;
      case FALSE -> unmatched;
      case ALL -> engine.executeUnion(List.of(matched, unmatched), List.of());
    };
  }

  private static DatasetExpression withBoolVar(
      DatasetExpression dataset, boolean value, Positioned position, ProcessingEngine engine) {
    ResolvableExpression constant =
        ResolvableExpression.withType(Boolean.class).withPosition(position).using(ctx -> value);
    return engine.executeCalc(
        dataset, Map.of(BOOL_VAR, constant), Map.of(BOOL_VAR, Dataset.Role.MEASURE), Map.of());
  }

  private static Retain parseRetain(VtlParser.RetainTypeContext retainCtx, Positioned position) {
    if (retainCtx == null || retainCtx.ALL() != null) {
      return Retain.ALL;
    }
    String text = retainCtx.BOOLEAN_CONSTANT().getText();
    if ("true".equalsIgnoreCase(text)) {
      return Retain.TRUE;
    }
    if ("false".equalsIgnoreCase(text)) {
      return Retain.FALSE;
    }
    throw new VtlRuntimeException(
        new InvalidArgumentException("exists_in retain must be true, false or all", position));
  }

  private static List<String> identifierNames(DatasetExpression dataset) {
    return dataset.getDataStructure().getIdentifiers().stream()
        .map(Component::getName)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  private static List<String> commonIdentifierNames(List<String> leftIds, List<String> rightIds) {
    Set<String> right = new LinkedHashSet<>(rightIds);
    List<String> common = new ArrayList<>();
    for (String id : leftIds) {
      if (right.contains(id)) {
        common.add(id);
      }
    }
    return common;
  }

  private static boolean containsAll(List<String> container, List<String> contained) {
    return new LinkedHashSet<>(container).containsAll(contained);
  }
}

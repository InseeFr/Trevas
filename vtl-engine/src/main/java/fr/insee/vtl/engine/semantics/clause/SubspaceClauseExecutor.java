package fr.insee.vtl.engine.semantics.clause;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.VtlScriptEngine.toPositioned;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.engine.semantics.aggregation.VtlParseTrees;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.engine.visitors.expression.ConstantVisitor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ConstantExpression;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Executes {@code [ sub identifier = value, … ]} (subspace) clauses. */
public final class SubspaceClauseExecutor {

  private static final ConstantVisitor CONSTANT_VISITOR = new ConstantVisitor();

  private SubspaceClauseExecutor() {}

  public static DatasetExpression execute(
      DatasetExpression input,
      VtlParser.SubspaceClauseContext ctx,
      ExpressionVisitor componentExpressionVisitor,
      ProcessingEngine processingEngine) {

    Structured.DataStructure structure = input.getDataStructure();
    Set<String> subIdentifiers = new LinkedHashSet<>();
    List<ResolvableExpression> predicates = new ArrayList<>();
    List<String> filterParts = new ArrayList<>();
    Positioned clausePosition = fromContext(ctx);

    for (VtlParser.SubspaceClauseItemContext item : ctx.subspaceClauseItem()) {
      String identifierName = VtlParseTrees.componentName(item.componentID());
      var itemPosition = fromContext(item);

      if (!subIdentifiers.add(identifierName)) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "duplicate identifier '%s'".formatted(identifierName),
                toPositioned(item.componentID())));
      }

      if (!structure.containsKey(identifierName)) {
        throw new VtlRuntimeException(
            new UndefinedVariableException(toPositioned(item.componentID()), input));
      }

      Structured.Component component = structure.get(identifierName);
      if (!component.isIdentifier()) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "sub can only fix identifier components", toPositioned(item.componentID())));
      }

      ConstantExpression constant = CONSTANT_VISITOR.visitConstant(item.constant());
      ComponentExpression componentExpression = new ComponentExpression(component, itemPosition);

      if (!TypeChecking.hasSameTypeOrNumberOrNull(List.of(componentExpression, constant))) {
        throw new VtlRuntimeException(
            new InvalidTypeException(
                component.getType(), constant.getType(), fromContext(item.constant())));
      }

      predicates.add(
          componentExpressionVisitor.invokeScalarFunction(
              "isEqual", List.of(componentExpression, constant), itemPosition));
      filterParts.add(VtlParseTrees.sourceText(item));
    }

    ResolvableExpression filter =
        combineWithAnd(predicates, clausePosition, componentExpressionVisitor);
    String filterString = String.join(" and ", filterParts);

    List<String> outputColumns =
        structure.keySet().stream()
            .filter(name -> !subIdentifiers.contains(name))
            .collect(Collectors.toList());

    DatasetExpression filtered = processingEngine.executeFilter(input, filter, filterString);
    return processingEngine.executeProject(filtered, outputColumns);
  }

  private static ResolvableExpression combineWithAnd(
      List<ResolvableExpression> predicates,
      Positioned position,
      ExpressionVisitor componentExpressionVisitor) {
    ResolvableExpression combined = predicates.get(0);
    for (int i = 1; i < predicates.size(); i++) {
      combined =
          componentExpressionVisitor.invokeScalarFunction(
              "and", List.of(combined, predicates.get(i)), position);
    }
    return combined;
  }
}

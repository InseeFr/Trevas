package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.RuleContext;

/**
 * <code>SetFunctionsVisitor</code> is the visitor for expressions involving set functions (i.e.
 * union).
 */
public class SetFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor expressionVisitor;

  private final ProcessingEngine processingEngine;

  /**
   * Constructor taking an expression visitor and a processing engine.
   *
   * @param expressionVisitor A visitor for the expression corresponding to the set function.
   * @param processingEngine The processing engine.
   */
  public SetFunctionsVisitor(
      ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
    this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    this.processingEngine = Objects.requireNonNull(processingEngine);
  }

  @Override
  public ResolvableExpression visitUnionAtom(VtlParser.UnionAtomContext ctx) {

    List<DatasetExpression> datasets = new ArrayList<>();
    Structured.DataStructure structure = null;
    for (VtlParser.ExprContext expr : ctx.expr()) {
      DatasetExpression rest =
          (DatasetExpression)
              assertTypeExpression(expressionVisitor.visit(expr), Dataset.class, expr);
      datasets.add(rest);

      // Check that all the structures are the same.
      if (structure == null) {
        structure = rest.getDataStructure();
      } else if (!structure.equals(rest.getDataStructure())) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "dataset structure of %s is incompatible with %s"
                    .formatted(
                        expr.getText(),
                        ctx.expr().stream()
                            .map(RuleContext::getText)
                            .collect(Collectors.joining(", "))),
                fromContext(ctx)));
      }
    }

    return processingEngine.executeUnion(datasets);
  }
}

package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import java.util.Objects;
import org.apache.commons.text.similarity.LevenshteinDistance;

/**
 * <code>DistanceFunctionsVisitor</code> is the base visitor for expressions involving distance
 * functions.
 */
public class DistanceFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;

  /**
   * Constructor taking an expression visitor.
   *
   * @param expressionVisitor The visitor for the enclosing expression.
   */
  public DistanceFunctionsVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

  public static Long levenshtein(String stringA, String stringB) {
    if (stringA == null || stringB == null) {
      return null;
    }
    return Long.valueOf(LevenshteinDistance.getDefaultInstance().apply(stringA, stringB));
  }

  /**
   * Visits a 'Levenshtein distance' expression with two strings parameters.
   *
   * @param ctx The scripting context for the expression (left and right expressions should be the
   *     string parameters).
   * @return A <code>ResolvableExpression</code> resolving to a long integer representing the
   *     Levenshtein distance between the parameters.
   */
  @Override
  public ResolvableExpression visitLevenshteinAtom(VtlParser.LevenshteinAtomContext ctx) {
    try {
      List<ResolvableExpression> parameters =
          List.of(exprVisitor.visit(ctx.left), exprVisitor.visit(ctx.right));
      return genericFunctionsVisitor.invokeFunction("levenshtein", parameters, fromContext(ctx));
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
}

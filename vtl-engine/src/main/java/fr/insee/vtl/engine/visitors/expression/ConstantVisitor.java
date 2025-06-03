package fr.insee.vtl.engine.visitors.expression;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.model.ConstantExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

/** <code>ConstantVisitor</code> is the base visitor for constant expressions. */
public class ConstantVisitor extends VtlBaseVisitor<ConstantExpression> {

  /**
   * Visits constant expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the constant value with the expected
   *     type.
   */
  @Override
  public ConstantExpression visitConstant(VtlParser.ConstantContext ctx) {
    Positioned pos = fromContext(ctx);
    if (ctx.INTEGER_CONSTANT() != null) {
      return new ConstantExpression(Long.parseLong(ctx.getText()), pos);
    }
    if (ctx.NUMBER_CONSTANT() != null) {
      return new ConstantExpression(Double.parseDouble(ctx.getText()), pos);
    }
    if (ctx.BOOLEAN_CONSTANT() != null) {
      return new ConstantExpression(Boolean.parseBoolean(ctx.getText()), pos);
    }
    if (ctx.STRING_CONSTANT() != null) {
      var text = ctx.getText();
      return new ConstantExpression(text.substring(1, text.length() - 1), pos);
    }
    if (ctx.NULL_CONSTANT() != null) {
      return new ConstantExpression(null, pos);
    }
    throw new UnsupportedOperationException("unknown constant type " + ctx);
  }
}

package fr.insee.vtl.engine.semantics.aggregation;

import fr.insee.vtl.antlr.runtime.ParserRuleContext;
import fr.insee.vtl.antlr.runtime.misc.Interval;
import fr.insee.vtl.parser.VtlParser;

/** Shared parse-tree helpers for clauses and aggregations. */
public final class VtlParseTrees {

  private VtlParseTrees() {}

  public static String componentName(VtlParser.ComponentIDContext context) {
    String text = context.getText();
    if (text.startsWith("'") && text.endsWith("'")) {
      text = text.substring(1, text.length() - 1);
    }
    return text;
  }

  /** Simple component reference inside an aggregate ({@code avg(Me_1)}). */
  public static String componentNameFromExpr(VtlParser.ExprContext expr) {
    if (expr instanceof VtlParser.VarIdExprContext varId) {
      return varId.varID().getText();
    }
    String text = expr.getText();
    if (text.startsWith("'") && text.endsWith("'")) {
      return text.substring(1, text.length() - 1);
    }
    return text;
  }

  public static String sourceText(ParserRuleContext ctx) {
    var stream = ctx.getStart().getInputStream();
    return stream.getText(
        new Interval(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex()));
  }
}

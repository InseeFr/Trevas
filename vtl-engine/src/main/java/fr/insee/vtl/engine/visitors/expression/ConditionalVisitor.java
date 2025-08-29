package fr.insee.vtl.engine.visitors.expression;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.hasSameTypeOrNull;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.stream.Collectors;


/** <code>IfVisitor</code> is the base visitor for if-then-else expressions. */
public class ConditionalVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor exprVisitor;

  private final GenericFunctionsVisitor genericFunctionsVisitor;

  /**
   * Constructor taking an expression visitor.
   *
   * @param expressionVisitor The visitor for the enclosing expression.
   * @param genericFunctionsVisitor
   */
  public ConditionalVisitor(
      ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
    this.exprVisitor = Objects.requireNonNull(expressionVisitor);
    this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
  }

  public static Long ifThenElse(Boolean condition, Long thenExpr, Long elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static Double ifThenElse(Boolean condition, Double thenExpr, Double elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static String ifThenElse(Boolean condition, String thenExpr, String elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static Boolean ifThenElse(Boolean condition, Boolean thenExpr, Boolean elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static Long nvl(Long value, Long defaultValue) {
    return value == null ? defaultValue : value;
  }

  public static Double nvl(Double value, Double defaultValue) {
    return value == null ? defaultValue : value;
  }

  public static Double nvl(Double value, Long defaultValue) {
    return value == null ? defaultValue.doubleValue() : value;
  }

  public static Double nvl(Long value, Double defaultValue) {
    return value == null ? defaultValue : value.doubleValue();
  }

  public static String nvl(String value, String defaultValue) {
    return value == null ? defaultValue : value;
  }

  public static Boolean nvl(Boolean value, Boolean defaultValue) {
    return value == null ? defaultValue : value;
  }

  /**
   * Visits if-then-else expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the if or else clause resolution
   *     depending on the condition resolution.
   */
  @Override
  public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {
    try {
      var conditionalExpr = exprVisitor.visit(ctx.conditionalExpr);
      var thenExpression = exprVisitor.visit(ctx.thenExpr);
      var elseExpression = exprVisitor.visit(ctx.elseExpr);
      Positioned position = fromContext(ctx);
      ResolvableExpression expression =
          genericFunctionsVisitor.invokeFunction(
              "ifThenElse", List.of(conditionalExpr, thenExpression, elseExpression), position);
      Class<?> actualType = thenExpression.getType();
      return new CastExpression(position, expression, actualType);
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
  
  @Override
  public ResolvableExpression visitCaseExpr(VtlParser.CaseExprContext ctx) {
    // Error reporting context (consistent with KEEP/DROP style).
    final int line = ctx.getStart().getLine();
    final int charPosition = ctx.getStart().getCharPositionInLine();
    final String statement = ctx.getText();
  
    try {
      Positioned pos = fromContext(ctx);
  
      // Collect raw expr nodes: WHEN cond THEN expr ... ELSE expr
      List<VtlParser.ExprContext> exprs = ctx.expr();
      if (exprs == null || exprs.size() < 3) {
        String errorMsg = String.format(
            "CASE requires at least one WHEN ... THEN pair and an ELSE expression. Line %d, position %d. Statement: [%s]",
            line, charPosition, statement);
        throw new VtlRuntimeException(new InvalidArgumentException(errorMsg, fromContext(ctx)));
      }
  
      // The last expression is ELSE; the preceding ones must come in WHEN/THEN pairs.
      int pairsPart = exprs.size() - 1;
      if (pairsPart % 2 != 0) {
        String errorMsg = String.format(
            "Malformed CASE expression: unmatched WHEN/THEN pair. Line %d, position %d. Statement: [%s]",
            line, charPosition, statement);
        throw new VtlRuntimeException(new InvalidArgumentException(errorMsg, fromContext(ctx)));
      }
  
      List<VtlParser.ExprContext> whenExprs = new ArrayList<>();
      List<VtlParser.ExprContext> thenExprs = new ArrayList<>();
      for (int i = 0; i < pairsPart; i += 2) {
        whenExprs.add(exprs.get(i));
        thenExprs.add(exprs.get(i + 1));
      }
  
      // Visit WHEN conditions and THEN branches
      List<ResolvableExpression> whenExpressions =
          whenExprs.stream().map(exprVisitor::visit).collect(Collectors.toList());
      List<ResolvableExpression> thenExpressions =
          thenExprs.stream().map(exprVisitor::visit).collect(Collectors.toList());
  
      // Visit ELSE branch
      ResolvableExpression elseExpression = exprVisitor.visit(exprs.get(exprs.size() - 1));
  
      // Validate: all WHEN conditions must be boolean
      for (int i = 0; i < whenExpressions.size(); i++) {
        Class<?> condType = whenExpressions.get(i).getType();
        if (condType != Boolean.class && condType != boolean.class) {
          String errorMsg = String.format(
              "CASE WHEN condition must be boolean, found: %s. Line %d, position %d. Statement: [%s]",
              condType == null ? "null" : condType.getName(), line, charPosition, statement);
          // Prefer InvalidTypeException if available
          throw new VtlRuntimeException(
              new InvalidTypeException(condType, Boolean.class, fromContext(whenExprs.get(i))));
          // (If you don't have InvalidTypeException, replace with InvalidArgumentException)
        }
      }
  
      // Validate: THEN and ELSE expressions must share the same type (or be null-compatible)
      List<ResolvableExpression> forTypeCheck = new ArrayList<>(thenExpressions);
      forTypeCheck.add(elseExpression);
      if (!hasSameTypeOrNull(forTypeCheck)) {
        // Build a readable list of found types
        String foundTypes = forTypeCheck.stream()
            .map(e -> {
              Class<?> t = e.getType();
              return t == null ? "null" : t.getName();
            })
            .distinct()
            .collect(Collectors.joining(", "));
  
        String errorMsg = String.format(
            "CASE THEN/ELSE branches must have the same type (or null-compatible). Found: %s. Line %d, position %d. Statement: [%s]",
            foundTypes, line, charPosition, statement);
        throw new VtlRuntimeException(new InvalidTypeException(
            // Use the first branch type as 'found' and require it to match across branches.
            forTypeCheck.get(0).getType(), forTypeCheck.get(0).getType(), fromContext(ctx)));
        // (If using InvalidArgumentException instead, keep the same message.)
      }
  
      // Output type is that of ELSE (all branches have been validated to match/allow null)
      Class<?> outputType = elseExpression.getType();
  
      // Delegate to existing builder: convert CASE to nested IF-THEN-ELSE and cast to output type.
      return new CastExpression(
          pos,
          caseToIfIt(
              whenExpressions.listIterator(), thenExpressions.listIterator(), elseExpression),
          outputType);
  
    } catch (VtlRuntimeException e) {
      throw e;
    } catch (Exception e) {
      String errorMsg = String.format(
          "Unexpected error while processing CASE expression at line %d, position %d. Statement: [%s]. Cause: %s",
          line, charPosition, statement, e.getMessage());
      throw new VtlRuntimeException(new VtlScriptException(errorMsg, fromContext(ctx)));
    }
  }

  private ResolvableExpression caseToIfIt(
      ListIterator<ResolvableExpression> whenExpr,
      ListIterator<ResolvableExpression> thenExpr,
      ResolvableExpression elseExpression) {
  
    try {
      // Structural guard: WHEN and THEN iterators must advance in lockstep.
      boolean hasWhen = whenExpr.hasNext();
      boolean hasThen = thenExpr.hasNext();
  
      if (!hasWhen && !hasThen) {
        // Base case: no more pairs -> return ELSE branch
        return elseExpression;
      }
  
      if (hasWhen != hasThen) {
        // Malformed CASE (should already be caught earlier, but keep defensive checks here)
        // Use the elseExpression as positional hint if available; otherwise this point is still valid.
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "Malformed CASE expression: unmatched WHEN/THEN pair.",
                elseExpression));
      }
  
      // Advance one WHEN…THEN pair
      ResolvableExpression nextWhen = whenExpr.next();
  
      // Type guard: WHEN must be boolean (defensive; main check is in visitCaseExpr)
      Class<?> condType = nextWhen.getType();
      if (condType != Boolean.class && condType != boolean.class) {
        throw new VtlRuntimeException(
            new InvalidTypeException(condType, Boolean.class, nextWhen));
      }
  
      if (!thenExpr.hasNext()) {
        // Extra safety (should not happen due to hasWhen == hasThen above)
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "Malformed CASE expression: THEN branch missing for a WHEN condition.",
                nextWhen));
      }
  
      ResolvableExpression nextThen = thenExpr.next();
  
      // Recurse for the remaining pairs, building nested ifThenElse
      ResolvableExpression elseTail = caseToIfIt(whenExpr, thenExpr, elseExpression);
  
      // Delegate to generic function with positional hint = nextWhen
      return genericFunctionsVisitor.invokeFunction(
          "ifThenElse",
          List.of(nextWhen, nextThen, elseTail),
          nextWhen);
  
    } catch (VtlRuntimeException e) {
      // Already in engine-native form; bubble up
      throw e;
    } catch (VtlScriptException e) {
      // Normalize script-layer errors to engine-native exceptions
      throw new VtlRuntimeException(e);
    } catch (Exception e) {
      // Last-resort normalization; use elseExpression/nextWhen as positional hint when possible
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "Unexpected error while converting CASE to IF-THEN-ELSE: " + e.getMessage(),
              elseExpression));
    }
  }

  /**
   * Visits nvl expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the null value clause.
   */
  @Override
  public ResolvableExpression visitNvlAtom(VtlParser.NvlAtomContext ctx) {
    try {
      ResolvableExpression expression = exprVisitor.visit(ctx.left);
      ResolvableExpression defaultExpression = exprVisitor.visit(ctx.right);

      Positioned position = fromContext(ctx);
      return genericFunctionsVisitor.invokeFunction(
          "nvl", List.of(expression, defaultExpression), position);
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  static class CastExpression extends ResolvableExpression {
    private final Class<?> type;
    private final ResolvableExpression expression;

    CastExpression(Positioned pos, ResolvableExpression expression, Class<?> type) {
      super(pos);
      this.type = type;
      this.expression = expression;
    }

    @Override
    public Object resolve(Map<String, Object> context) {
      return type.cast(expression.resolve(context));
    }

    @Override
    public Class<?> getType() {
      return type;
    }
  }
}

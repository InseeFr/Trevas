package fr.insee.vtl.prov2;

import fr.insee.vtl.antlr.runtime.tree.RuleNode;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

/**
 * Grammar-only support gate: throws {@code unsupported: …} before the structure oracle runs, so the
 * corpus backlog stays explicit even when the engine cannot eval the script.
 *
 * <p>Supported so far: identity assign, binary dataset arithmetic (leaf operands), single {@code
 * calc} / {@code filter} / {@code sub} / {@code keep}/{@code drop} / {@code rename} clause on a
 * dataset varId.
 */
class SupportCheckVisitor extends VtlBaseVisitor<Void> {

  @Override
  public Void visitChildren(RuleNode node) {
    throw unsupported(node.getClass().getSimpleName());
  }

  @Override
  public Void visitStart(VtlParser.StartContext ctx) {
    for (VtlParser.StatementContext statement : ctx.statement()) {
      visit(statement);
    }
    return null;
  }

  @Override
  public Void visitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
    return visit(ctx.expr());
  }

  @Override
  public Void visitPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
    return visit(ctx.expr());
  }

  @Override
  public Void visitDefineExpression(VtlParser.DefineExpressionContext ctx) {
    throw unsupported("define");
  }

  @Override
  public Void visitParenthesisExpr(VtlParser.ParenthesisExprContext ctx) {
    return visit(ctx.expr());
  }

  @Override
  public Void visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
    return null;
  }

  @Override
  public Void visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
    return binaryArithmetic(ctx.left, ctx.right);
  }

  @Override
  public Void visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext ctx) {
    return binaryArithmetic(ctx.left, ctx.right);
  }

  @Override
  public Void visitUnaryExpr(VtlParser.UnaryExprContext ctx) {
    throw unsupported("arithmetic");
  }

  @Override
  public Void visitClauseExpr(VtlParser.ClauseExprContext ctx) {
    datasetVarId(ctx.expr());
    VtlParser.DatasetClauseContext clause = ctx.datasetClause();
    if (clause.calcClause() != null) {
      for (VtlParser.CalcClauseItemContext item : clause.calcClause().calcClauseItem()) {
        calcRhs(item.expr());
      }
      return null;
    }
    if (clause.filterClause() != null) {
      scalarPredicate(clause.filterClause().expr());
      return null;
    }
    if (clause.subspaceClause() != null) {
      return null;
    }
    if (clause.keepOrDropClause() != null || clause.renameClause() != null) {
      return null;
    }
    throw unsupported("clause");
  }

  @Override
  public Void visitFunctionsExpression(VtlParser.FunctionsExpressionContext ctx) {
    throw unsupported("functions");
  }

  @Override
  public Void visitConstantExpr(VtlParser.ConstantExprContext ctx) {
    throw unsupported("scalar");
  }

  private Void binaryArithmetic(VtlParser.ExprContext left, VtlParser.ExprContext right) {
    leafOperand(left);
    leafOperand(right);
    return null;
  }

  /** Dataset name or scalar literal; nested ops deferred. */
  private void leafOperand(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.VarIdExprContext) {
      return;
    }
    if (current instanceof VtlParser.ConstantExprContext) {
      return;
    }
    throw unsupported("arithmetic");
  }

  private void datasetVarId(VtlParser.ExprContext expr) {
    if (!(unwrap(expr) instanceof VtlParser.VarIdExprContext)) {
      throw unsupported("clause");
    }
  }

  /** Component-level calc RHS (not dataset-level arithmetic). */
  private void calcRhs(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.VarIdExprContext
        || current instanceof VtlParser.ConstantExprContext) {
      return;
    }
    if (current instanceof VtlParser.ArithmeticExprContext arithmetic) {
      calcRhs(arithmetic.left);
      calcRhs(arithmetic.right);
      return;
    }
    if (current instanceof VtlParser.ArithmeticExprOrConcatContext arithmetic) {
      calcRhs(arithmetic.left);
      calcRhs(arithmetic.right);
      return;
    }
    throw unsupported("clause");
  }

  /** Filter predicates may use functions/comparisons; reject nested dataset clauses only. */
  private void scalarPredicate(VtlParser.ExprContext expr) {
    new VtlBaseVisitor<Void>() {
      @Override
      public Void visitClauseExpr(VtlParser.ClauseExprContext ctx) {
        throw unsupported("clause");
      }
    }.visit(expr);
  }

  static VtlParser.ExprContext unwrap(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = expr;
    while (current instanceof VtlParser.ParenthesisExprContext parenthesis) {
      current = parenthesis.expr();
    }
    return current;
  }

  static UnsupportedOperationException unsupported(String what) {
    return new UnsupportedOperationException("unsupported: " + what);
  }
}

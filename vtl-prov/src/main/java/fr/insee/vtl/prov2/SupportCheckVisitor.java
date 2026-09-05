package fr.insee.vtl.prov2;

import fr.insee.vtl.antlr.runtime.tree.RuleNode;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;

/**
 * Grammar-only support gate: throws {@code unsupported: …} before the structure oracle runs, so the
 * corpus backlog stays explicit even when the engine cannot eval the script.
 *
 * <p>Mirrors {@link ProvenanceVisitor} coverage. Message vocabulary (stable for harness / ops):
 * {@code define}, {@code scalar}, {@code arithmetic}, {@code clause}, {@code calc}, {@code aggr},
 * {@code join}, {@code set}, {@code functions} (catch-all for other function families).
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
    leafOperand(ctx.left);
    leafOperand(ctx.right);
    return null;
  }

  @Override
  public Void visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext ctx) {
    leafOperand(ctx.left);
    leafOperand(ctx.right);
    return null;
  }

  @Override
  public Void visitUnaryExpr(VtlParser.UnaryExprContext ctx) {
    throw unsupported("arithmetic");
  }

  @Override
  public Void visitClauseExpr(VtlParser.ClauseExprContext ctx) {
    requireDatasetOrClause(ctx.expr());
    VtlParser.DatasetClauseContext clause = ctx.datasetClause();
    if (clause.calcClause() != null) {
      clause.calcClause().calcClauseItem().forEach(item -> calcRhs(item.expr()));
      return null;
    }
    if (clause.filterClause() != null) {
      requireScalarPredicate(clause.filterClause().expr());
      return null;
    }
    if (clause.aggrClause() != null) {
      return null;
    }
    if (clause.subspaceClause() != null
        || clause.keepOrDropClause() != null
        || clause.renameClause() != null) {
      return null;
    }
    throw unsupported("clause");
  }

  @Override
  public Void visitFunctionsExpression(VtlParser.FunctionsExpressionContext ctx) {
    if (ctx.functions() instanceof VtlParser.JoinFunctionsContext join) {
      return visit(join);
    }
    if (ctx.functions() instanceof VtlParser.SetFunctionsContext set) {
      return visit(set);
    }
    throw unsupported("functions");
  }

  @Override
  public Void visitJoinFunctions(VtlParser.JoinFunctionsContext ctx) {
    return visit(ctx.joinOperators());
  }

  @Override
  public Void visitJoinExpr(VtlParser.JoinExprContext ctx) {
    requireEmptyJoinBody(ctx.joinBody());
    for (VtlParser.JoinClauseItemContext item : joinItems(ctx)) {
      if (item.AS() != null) {
        throw unsupported("join");
      }
      requireDatasetVarId(item.expr(), "join");
    }
    return null;
  }

  @Override
  public Void visitSetFunctions(VtlParser.SetFunctionsContext ctx) {
    return visit(ctx.setOperators());
  }

  @Override
  public Void visitUnionAtom(VtlParser.UnionAtomContext ctx) {
    ctx.expr().forEach(e -> requireDatasetVarId(e, "set"));
    return null;
  }

  @Override
  public Void visitIntersectAtom(VtlParser.IntersectAtomContext ctx) {
    ctx.expr().forEach(e -> requireDatasetVarId(e, "set"));
    return null;
  }

  @Override
  public Void visitSetOrSYmDiffAtom(VtlParser.SetOrSYmDiffAtomContext ctx) {
    requireDatasetVarId(ctx.left, "set");
    requireDatasetVarId(ctx.right, "set");
    return null;
  }

  @Override
  public Void visitConstantExpr(VtlParser.ConstantExprContext ctx) {
    throw unsupported("scalar");
  }

  /** Dataset name only (no nested clause / expression operands yet). */
  private void requireDatasetVarId(VtlParser.ExprContext expr, String what) {
    if (!(unwrap(expr) instanceof VtlParser.VarIdExprContext)) {
      throw unsupported(what);
    }
  }

  static List<VtlParser.JoinClauseItemContext> joinItems(VtlParser.JoinExprContext ctx) {
    if (ctx.joinClause() != null) {
      return ctx.joinClause().joinClauseItem();
    }
    return ctx.joinClauseWithoutUsing().joinClauseItem();
  }

  static void requireEmptyJoinBody(VtlParser.JoinBodyContext body) {
    if (body == null) {
      return;
    }
    if (body.filterClause() != null
        || body.calcClause() != null
        || body.joinApplyClause() != null
        || body.aggrClause() != null
        || body.keepOrDropClause() != null
        || body.renameClause() != null) {
      throw unsupported("join");
    }
  }

  /** Dataset name or scalar literal; nested ops deferred. */
  private void leafOperand(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.VarIdExprContext
        || current instanceof VtlParser.ConstantExprContext) {
      return;
    }
    throw unsupported("arithmetic");
  }

  /** Dataset name, or a nested clause chain ({@code ds[…][…]}). */
  private void requireDatasetOrClause(VtlParser.ExprContext expr) {
    VtlParser.ExprContext current = unwrap(expr);
    if (current instanceof VtlParser.VarIdExprContext) {
      return;
    }
    if (current instanceof VtlParser.ClauseExprContext clause) {
      visit(clause);
      return;
    }
    throw unsupported("clause");
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
    if (current instanceof VtlParser.FunctionsExpressionContext functions
        && functions.functions() instanceof VtlParser.AnalyticFunctionsContext) {
      return;
    }
    throw unsupported("calc");
  }

  /** Filter predicates may use functions/comparisons; reject nested dataset clauses only. */
  private void requireScalarPredicate(VtlParser.ExprContext expr) {
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

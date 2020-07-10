package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.visitors.ClauseVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.ComparisonFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.StringFunctionsVisitor;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;
import java.util.Objects;

// TODO: Reuse instance of visitors.
public class ExpressionVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ScriptContext context;

    public ExpressionVisitor(ScriptContext context) {
        this.context = Objects.requireNonNull(context);
    }

    @Override
    public ResolvableExpression visitConstant(VtlParser.ConstantContext ctx) {
        return new ConstantVisitor().visit(ctx);
    }

    @Override
    public ResolvableExpression visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
        return new VarIdVisitor(context).visit(ctx);
    }

    @Override
    public ResolvableExpression visitBooleanExpr(VtlParser.BooleanExprContext ctx) {
        return new BooleanVisitor(context).visit(ctx);
    }

    @Override
    public ResolvableExpression visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
        return new ArithmeticVisitor(context).visit(ctx);
    }

    @Override
    public ResolvableExpression visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext ctx) {
        return new ArithmeticExprOrConcatVisitor(context).visit(ctx);
    }

    @Override
    public ResolvableExpression visitParenthesisExpr(VtlParser.ParenthesisExprContext ctx) {
        return visit(ctx.expr());
    }

    @Override
    public ResolvableExpression visitComparisonExpr(VtlParser.ComparisonExprContext ctx) {
        return new ComparisonVisitor(context).visit(ctx);
    }

    @Override
    public ResolvableExpression visitInNotInExpr(VtlParser.InNotInExprContext ctx) {
        return new ComparisonVisitor(context).visit(ctx);
    }

    @Override
    public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {
        return new IfVisitor(context).visit(ctx);
    }

    /*
    Functions
     */

    @Override
    public ResolvableExpression visitStringFunctions(VtlParser.StringFunctionsContext ctx) {
        return new StringFunctionsVisitor(context).visit(ctx.stringOperators());
    }

    @Override
    public ResolvableExpression visitComparisonFunctions(VtlParser.ComparisonFunctionsContext ctx) {
        return new ComparisonFunctionsVisitor(context).visit(ctx.comparisonOperators());
    }

    @Override
    public ResolvableExpression visitClauseExpr(VtlParser.ClauseExprContext ctx) {
        DatasetExpression datasetExpression = (DatasetExpression) visit(ctx.dataset);

        ClauseVisitor clauseVisitor = new ClauseVisitor(datasetExpression);
        return clauseVisitor.visit(ctx.clause);
    }
}

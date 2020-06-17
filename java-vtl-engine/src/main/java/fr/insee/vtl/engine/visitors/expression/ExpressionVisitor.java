package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.visitors.expression.functions.StringFunctionsVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

public class ExpressionVisitor extends VtlBaseVisitor<ResolvableExpression> {

    @Override
    public ResolvableExpression visitConstant(VtlParser.ConstantContext ctx) {
        return new ConstantVisitor().visit(ctx);
    }

    @Override
    public ResolvableExpression visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
        return new VarIdVisitor().visit(ctx);
    }

    @Override
    public ResolvableExpression visitBooleanExpr(VtlParser.BooleanExprContext ctx) {
        return new BooleanVisitor().visit(ctx);
    }

    @Override
    public ResolvableExpression visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
        return new ArithmeticVisitor().visit(ctx);
    }

    @Override
    public ResolvableExpression visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext ctx) {
        return new ArithmeticExprOrConcatVisitor().visit(ctx);
    }

    @Override
    public ResolvableExpression visitParenthesisExpr(VtlParser.ParenthesisExprContext ctx) {
        return visit(ctx.expr());
    }

    @Override
    public ResolvableExpression visitComparisonExpr(VtlParser.ComparisonExprContext ctx) {
        return new ComparisonVisitor().visit(ctx);
    }

    /*
    Functions
     */

    @Override
    public ResolvableExpression visitStringFunctions(VtlParser.StringFunctionsContext ctx) {
        return new StringFunctionsVisitor().visit(ctx.stringOperators());
    }
}

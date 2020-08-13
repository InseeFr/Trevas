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

public class ExpressionVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private static final ConstantVisitor CONSTANT_VISITOR = new ConstantVisitor();
    private final VarIdVisitor varIdVisitor;
    private final BooleanVisitor booleanVisitor;
    private final ArithmeticVisitor arithmeticVisitor;
    private final ArithmeticExprOrConcatVisitor arithmeticExprOrConcatVisitor;
    private final UnaryVisitor unaryVisitor;
    private final ComparisonVisitor comparisonVisitor;
    private final IfVisitor ifVisitor;
    private final StringFunctionsVisitor stringFunctionsVisitor;
    private final ComparisonFunctionsVisitor comparisonFunctionsVisitor;

    public ExpressionVisitor(ScriptContext context) {
        Objects.requireNonNull(context);
        varIdVisitor = new VarIdVisitor(context);
        booleanVisitor = new BooleanVisitor(this);
        arithmeticVisitor = new ArithmeticVisitor(this);
        arithmeticExprOrConcatVisitor = new ArithmeticExprOrConcatVisitor(this);
        unaryVisitor = new UnaryVisitor(this);
        comparisonVisitor = new ComparisonVisitor(this);
        ifVisitor = new IfVisitor(this);
        stringFunctionsVisitor = new StringFunctionsVisitor(this);
        comparisonFunctionsVisitor = new ComparisonFunctionsVisitor(this);
    }

    @Override
    public ResolvableExpression visitConstant(VtlParser.ConstantContext ctx) {
        return CONSTANT_VISITOR.visit(ctx);
    }

    @Override
    public ResolvableExpression visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
        return varIdVisitor.visit(ctx);
    }

    @Override
    public ResolvableExpression visitBooleanExpr(VtlParser.BooleanExprContext ctx) {
        return booleanVisitor.visit(ctx);
    }

    @Override
    public ResolvableExpression visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
        return arithmeticVisitor.visit(ctx);
    }

    @Override
    public ResolvableExpression visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext ctx) {
        return arithmeticExprOrConcatVisitor.visit(ctx);
    }

    @Override
    public ResolvableExpression visitUnaryExpr(VtlParser.UnaryExprContext ctx) {
        return unaryVisitor.visit(ctx);
    }

    @Override
    public ResolvableExpression visitParenthesisExpr(VtlParser.ParenthesisExprContext ctx) {
        return visit(ctx.expr());
    }

    @Override
    public ResolvableExpression visitComparisonExpr(VtlParser.ComparisonExprContext ctx) {
        return comparisonVisitor.visit(ctx);
    }

    @Override
    public ResolvableExpression visitInNotInExpr(VtlParser.InNotInExprContext ctx) {
        return comparisonVisitor.visit(ctx);
    }

    @Override
    public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {
        return ifVisitor.visit(ctx);
    }

    /*
    Functions
     */

    @Override
    public ResolvableExpression visitStringFunctions(VtlParser.StringFunctionsContext ctx) {
        return stringFunctionsVisitor.visit(ctx.stringOperators());
    }

    @Override
    public ResolvableExpression visitComparisonFunctions(VtlParser.ComparisonFunctionsContext ctx) {
        return comparisonFunctionsVisitor.visit(ctx.comparisonOperators());
    }

    @Override
    public ResolvableExpression visitClauseExpr(VtlParser.ClauseExprContext ctx) {
        DatasetExpression datasetExpression = (DatasetExpression) visit(ctx.dataset);
        ClauseVisitor clauseVisitor = new ClauseVisitor(datasetExpression);
        return clauseVisitor.visit(ctx.clause);
    }
}

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

/**
 * <code>ExpressionVisitor</code> is the base visitor for expressions.
 * It essentially passes the expressions to the more specialized visitors defined in the package.
 */
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

    /**
     * Constructor taking a scripting context.
     *
     * @param context The scripting context for the visitor.
     */
    public ExpressionVisitor(ScriptContext context) {
        Objects.requireNonNull(context);
        // TODO Wouldn't it be more efficient to initialize specialized visitors in their own method?
        varIdVisitor = new VarIdVisitor(context);
        booleanVisitor = new BooleanVisitor(context);
        arithmeticVisitor = new ArithmeticVisitor(context);
        arithmeticExprOrConcatVisitor = new ArithmeticExprOrConcatVisitor(context);
        unaryVisitor = new UnaryVisitor(context);
        comparisonVisitor = new ComparisonVisitor(context);
        ifVisitor = new IfVisitor(context);
        stringFunctionsVisitor = new StringFunctionsVisitor(context);
        comparisonFunctionsVisitor = new ComparisonFunctionsVisitor(context);
    }

    /**
     * Visits constants expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the constant value with the expected type.
     * @see ConstantVisitor#visitConstant(VtlParser.ConstantContext)
     */
    @Override
    public ResolvableExpression visitConstant(VtlParser.ConstantContext ctx) {
        return CONSTANT_VISITOR.visit(ctx);
    }

    /**
     * Visits expressions with variable identifiers.
     * 
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> or more specialized child resolving to the value of the variable.
     * @see VarIdVisitor#visitVarIdExpr(VtlParser.VarIdExprContext) 
     */
    @Override
    public ResolvableExpression visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
        return varIdVisitor.visit(ctx);
    }

    /**
     * Visits expressions with boolean operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the boolean operation.
     * @see BooleanVisitor#visitBooleanExpr(VtlParser.BooleanExprContext) 
     */
    @Override
    public ResolvableExpression visitBooleanExpr(VtlParser.BooleanExprContext ctx) {
        return booleanVisitor.visit(ctx);
    }

    /**
     * Visits expressions with multiplication or division operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the multiplication or division operation.
     * @see ArithmeticVisitor#visitArithmeticExpr(VtlParser.ArithmeticExprContext) 
     */
    @Override
    public ResolvableExpression visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
        return arithmeticVisitor.visit(ctx);
    }

    /**
     * Visits expressions with plus, minus or concatenation operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the plus, minus or concatenation operation.
     * @see ArithmeticExprOrConcatVisitor#visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext) 
     */
    @Override
    public ResolvableExpression visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext ctx) {
        return arithmeticExprOrConcatVisitor.visit(ctx);
    }

    /**
     * Visits unary expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the unary operation.
     * @see UnaryVisitor#visitUnaryExpr(VtlParser.UnaryExprContext)
     */
    @Override
    public ResolvableExpression visitUnaryExpr(VtlParser.UnaryExprContext ctx) {
        return unaryVisitor.visit(ctx);
    }

    /**
     * Visits expressions between parentheses (just passes the expression down the tree).
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> corresponding to the expression between parentheses.
     */
    @Override
    public ResolvableExpression visitParenthesisExpr(VtlParser.ParenthesisExprContext ctx) {
        return visit(ctx.expr());
    }

    /**
     * Visits expressions with comparisons.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the boolean result of the comparison.
     * @see ComparisonVisitor#visitComparisonExpr(VtlParser.ComparisonExprContext) 
     */
    @Override
    public ResolvableExpression visitComparisonExpr(VtlParser.ComparisonExprContext ctx) {
        return comparisonVisitor.visit(ctx);
    }

    /**
     * Visits 'element of' ('In' or 'Not in') expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the boolean result of the 'element of' expression.
     * @see ComparisonVisitor#visitInNotInExpr(VtlParser.InNotInExprContext)
     */
    @Override
    public ResolvableExpression visitInNotInExpr(VtlParser.InNotInExprContext ctx) {
        return comparisonVisitor.visit(ctx);
    }

    /**
     * Visits if-then-else expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the if or else clause resolution depending on the condition resolution.
     * @see IfVisitor#visitIfExpr(VtlParser.IfExprContext)
     */
    @Override
    public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {
        return ifVisitor.visit(ctx);
    }

    /*
    Functions
     */

    /**
     * Visits expressions involving string functions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the string function.
     * @see StringFunctionsVisitor
     */
    @Override
    public ResolvableExpression visitStringFunctions(VtlParser.StringFunctionsContext ctx) {
        return stringFunctionsVisitor.visit(ctx.stringOperators());
    }

    /**
     * Visits expressions involving comparison functions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the comparison function.
     * @see ComparisonFunctionsVisitor
     */
    @Override
    public ResolvableExpression visitComparisonFunctions(VtlParser.ComparisonFunctionsContext ctx) {
        return comparisonFunctionsVisitor.visit(ctx.comparisonOperators());
    }

    /**
     * Visits clause expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the close expression.
     * @see ClauseVisitor
     */
    @Override
    public ResolvableExpression visitClauseExpr(VtlParser.ClauseExprContext ctx) {
        DatasetExpression datasetExpression = (DatasetExpression) visit(ctx.dataset);
        ClauseVisitor clauseVisitor = new ClauseVisitor(datasetExpression);
        return clauseVisitor.visit(ctx.clause);
    }
}

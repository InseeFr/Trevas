package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.isNull;

/**
 * <code>IfVisitor</code> is the base visitor for if-then-else expressions.
 */
public class ConditionalVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    private final GenericFunctionsVisitor genericFunctionsVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor       The visitor for the enclosing expression.
     * @param genericFunctionsVisitor
     */
    public ConditionalVisitor(ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
        this.exprVisitor = Objects.requireNonNull(expressionVisitor);
        this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
    }

    /**
     * Visits if-then-else expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the if or else clause resolution depending on the condition resolution.
     */
    @Override
    public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {
        try {
            var conditionalExpr = exprVisitor.visit(ctx.conditionalExpr);
            var thenExpression = exprVisitor.visit(ctx.thenExpr);
            var elseExpression = exprVisitor.visit(ctx.elseExpr);
            Positioned position = fromContext(ctx);
            ResolvableExpression expression = genericFunctionsVisitor.invokeFunction("ifThenElse", List.of(conditionalExpr, thenExpression, elseExpression), position);
            Class<?> actualType = thenExpression.getType();
            return new CastExpresison(position, expression, actualType);
        } catch (VtlScriptException e) {
            // Is FunctionNotFoundException actually type exception?
            throw new RuntimeException(e);
        }
    }

    static class CastExpresison extends ResolvableExpression {
        private final Class<?> type;
        private final ResolvableExpression expression;

        CastExpresison(Positioned pos, ResolvableExpression expression, Class<?> type) {
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

    /**
     * Visits nvl expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the null value clause.
     */
    @Override
    public ResolvableExpression visitNvlAtom(VtlParser.NvlAtomContext ctx) {
        ResolvableExpression expression = exprVisitor.visit(ctx.left);
        ResolvableExpression defaultExpression = exprVisitor.visit(ctx.right);

        if (isNull(expression)) {
            return defaultExpression;
        }

        return new NvlExpression(fromContext(ctx), expression, defaultExpression);
    }

    public static <T> T ifThenElse(Boolean condition, T thenExpr, T elseExpr) {
        if (condition == null) {
            return null;
        }
        return condition ? thenExpr : elseExpr;
    }

    static class IfThenExpression extends ResolvableExpression {

        final ResolvableExpression conditionExpr;
        final ResolvableExpression thenExpr;
        final ResolvableExpression elseExpr;

        final Class<?> type;

        IfThenExpression(Positioned position,
                         ResolvableExpression conditionExpr,
                         ResolvableExpression thenExpr,
                         ResolvableExpression elseExpr) throws InvalidTypeException {
            super(position);
            this.conditionExpr = conditionExpr.checkInstanceOf(Boolean.class);

            // Normalize the types of the branches.
            if (!isNull(thenExpr)) {
                elseExpr = elseExpr.checkInstanceOf(thenExpr.getType()).tryCast(thenExpr.getType());
            }
            if (!isNull(elseExpr)) {
                thenExpr = thenExpr.checkInstanceOf(elseExpr.getType()).tryCast(elseExpr.getType());
            }

            this.thenExpr = thenExpr;
            this.elseExpr = elseExpr;
            this.type = thenExpr.getType();
        }

        @Override
        public Object resolve(Map<String, Object> context) {
            if (isNull(conditionExpr)) {
                return null;
            }
            var cond = (Boolean) conditionExpr.resolve(context);
            if (cond == null) {
                return null;
            }
            return Boolean.TRUE.equals(cond)
                    ? thenExpr.resolve(context)
                    : elseExpr.resolve(context);
        }

        @Override
        public Class<?> getType() {
            return type;
        }
    }

    static class NvlExpression extends ResolvableExpression {

        private final ResolvableExpression defaultExpr;
        private final ResolvableExpression expr;

        protected NvlExpression(Positioned positioned, ResolvableExpression expr, ResolvableExpression defaultExpr) {
            super(positioned);
            try {
                defaultExpr.checkInstanceOf(expr.getType()).tryCast(expr.getType());
                expr.checkInstanceOf(defaultExpr.getType());
            } catch (InvalidTypeException e) {
                throw new VtlRuntimeException(e);
            }
            this.expr = expr;
            this.defaultExpr = defaultExpr;

        }

        @Override
        public Object resolve(Map<String, Object> context) {
            var resolvedExpression = expr.resolve(context);
            return resolvedExpression == null ?
                    defaultExpr.resolve(context) :
                    resolvedExpression;
        }

        @Override
        public Class<?> getType() {
            return expr.getType();
        }
    }
}

package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ConstantExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.time.Instant;
import java.util.List;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>TimeFunctionsVisitor</code> is the base visitor for expressions involving time functions.
 */
public class TimeFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final GenericFunctionsVisitor genericFunctionsVisitor;
    private final ExpressionVisitor expressionVisitor;

    public TimeFunctionsVisitor(GenericFunctionsVisitor genericFunctionsVisitor, ExpressionVisitor expressionVisitor) {
        this.genericFunctionsVisitor = genericFunctionsVisitor;
        this.expressionVisitor = expressionVisitor;
    }

    /**
     * Visits the current date expression.
     *
     * @param ctx The scripting context for the expression (left and right expressions should be the string parameters).
     * @return A <code>ResolvableExpression</code> resolving to a long integer representing the Levenshtein distance between the parameters.
     */
    @Override
    public ResolvableExpression visitCurrentDateAtom(VtlParser.CurrentDateAtomContext ctx) {
        return new ConstantExpression(Instant.now(), fromContext(ctx));
    }

    @Override
    public ResolvableExpression visitTimeShiftAtom(VtlParser.TimeShiftAtomContext ctx) {
        try {
            // signed integer is a special rule, so we cannot rely on the expression visitor. This means that the
            // second parameter must be a signed integer literal.

            // TODO: This does not work since this operator changes the identifier value.

            return genericFunctionsVisitor.invokeFunction("timeshift", List.of(
                    expressionVisitor.visit(ctx.expr()),
                    new ConstantExpression(Long.parseLong(ctx.signedInteger().getText()), fromContext(ctx.signedInteger()))
            ), fromContext(ctx));
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }
}

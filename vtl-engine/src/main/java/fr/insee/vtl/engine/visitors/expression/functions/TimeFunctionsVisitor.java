package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.threeten.extra.Interval;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>TimeFunctionsVisitor</code> is the base visitor for expressions involving time functions.
 */
public class TimeFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final GenericFunctionsVisitor genericFunctionsVisitor;
    private final ExpressionVisitor expressionVisitor;
    private final ProcessingEngine processingEngine;

    public TimeFunctionsVisitor(GenericFunctionsVisitor genericFunctionsVisitor, ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
        this.genericFunctionsVisitor = genericFunctionsVisitor;
        this.expressionVisitor = expressionVisitor;
        this.processingEngine = processingEngine;
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
            ResolvableExpression operand = expressionVisitor.visit(ctx.expr());
            ConstantExpression n = new ConstantExpression(Long.parseLong(ctx.signedInteger().getText()), fromContext(ctx.signedInteger()));

            // Fall through if not dataset.
            if (!(operand instanceof DatasetExpression)) {
                return genericFunctionsVisitor.invokeFunction("timeshift", List.of(
                        operand,
                        n
                ), fromContext(ctx));
            }

            DatasetExpression ds = (DatasetExpression) operand;

            // Find the time column
            var t = ds.getIdentifiers().stream()
                    .filter(component -> component.getType().equals(Interval.class))
                    .findFirst()
                    .orElseThrow(() -> new InvalidArgumentException("no time column in ", fromContext(ctx.expr())));

            var compExpr = genericFunctionsVisitor.invokeFunction("timeshift", List.of(
                    new ComponentExpression(t, fromContext(ctx)),
                    n
            ), fromContext(ctx));;
            return processingEngine.executeCalc(ds, Map.of(t.getName(), compExpr), Map.of(t.getName(), t.getRole()), Map.of());

        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }
}

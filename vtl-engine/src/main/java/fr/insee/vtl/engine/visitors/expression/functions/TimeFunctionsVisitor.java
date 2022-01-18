package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.time.LocalDate;

/**
 * <code>TimeFunctionsVisitor</code> is the base visitor for expressions involving time functions.
 */
public class TimeFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    /**
     * Constructor taking a scripting context.
     *
     * @param context The scripting context for the visitor.
     */
    public TimeFunctionsVisitor() {
    }

    /**
     * Visits the current date expression.
     *
     * @param ctx The scripting context for the expression (left and right expressions should be the string parameters).
     * @return A <code>ResolvableExpression</code> resolving to a long integer representing the Levenshtein distance between the parameters.
     */
    @Override
    public ResolvableExpression visitCurrentDateAtom(VtlParser.CurrentDateAtomContext ctx) {
        return ResolvableExpression.withType(LocalDate.class, context -> LocalDate.now());
    }
}

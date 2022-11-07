package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Map;
import java.util.Objects;

/**
 * <code>ValidationFunctionsVisitor</code> is the base visitor for expressions involving validation functions.
 */
public class ValidationFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor expressionVisitor;
    private final ProcessingEngine processingEngine;

    /**
     * Constructor taking an expression visitor and a processing engine.
     *
     * @param expressionVisitor A visitor for the expression corresponding to the validation function.
     * @param processingEngine  The processing engine.
     */
    public ValidationFunctionsVisitor(ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
        this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
        this.processingEngine = Objects.requireNonNull(processingEngine);
    }

    /**
     * Visits the current date expression.
     *
     * @param ctx The scripting context for the expression...
     * @return A <code>ResolvableExpression</code> resolving to...
     */
    @Override
    public ResolvableExpression visitValidateDPruleset(VtlParser.ValidateDPrulesetContext ctx) {

        return processingEngine.executeValidateDPruleset(new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                return null;
            }

            @Override
            public DataStructure getDataStructure() {
                return null;
            }
        });
    }
}

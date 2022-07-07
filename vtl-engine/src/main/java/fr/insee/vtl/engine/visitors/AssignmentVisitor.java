package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.Bindings;
import javax.script.ScriptContext;
import java.util.Objects;

/**
 * <code>AssignmentVisitor</code> is the visitor for VTL assignment expressions.
 */
public class AssignmentVisitor extends VtlBaseVisitor<Object> {

    private final ScriptContext context;
    private final ExpressionVisitor expressionVisitor;

    /**
     * Constructor taking a scripting context and a processing engine.
     *
     * @param context The scripting context.
     * @param processingEngine The processing engine.
     */
    public AssignmentVisitor(ScriptContext context, ProcessingEngine processingEngine) {
        this.context = Objects.requireNonNull(context);
        expressionVisitor = new ExpressionVisitor(
                context.getBindings(ScriptContext.ENGINE_SCOPE),
                processingEngine,
                context);
    }

    @Override
    public Object visitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        ResolvableExpression resolvableExpression = expressionVisitor.visit(ctx.expr());
        Bindings bindings = context.getBindings(ScriptContext.ENGINE_SCOPE);
        Object assignedObject = resolvableExpression.resolve(bindings);
        String variableIdentifier = ctx.varID().getText();
        bindings.put(variableIdentifier, assignedObject);
        return assignedObject;
    }
}

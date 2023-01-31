package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.DataPointRule;
import fr.insee.vtl.model.DataPointRuleset;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.Bindings;
import javax.script.ScriptContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <code>AssignmentVisitor</code> is the visitor for VTL assignment expressions.
 */
public class AssignmentVisitor extends VtlBaseVisitor<Object> {

    private final VtlScriptEngine engine;
    private final ProcessingEngine processingEngine;
    private final ExpressionVisitor expressionVisitor;

    /**
     * Constructor taking a scripting engine and a processing engine.
     *
     * @param engine           The scripting engine.
     * @param processingEngine The processing engine.
     */
    public AssignmentVisitor(VtlScriptEngine engine, ProcessingEngine processingEngine) {
        this.engine = Objects.requireNonNull(engine);
        this.processingEngine = Objects.requireNonNull(processingEngine);
        expressionVisitor = new ExpressionVisitor(
                engine.getBindings(ScriptContext.ENGINE_SCOPE),
                processingEngine,
                engine);
    }

    @Override
    public Object visitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        ResolvableExpression resolvableExpression = expressionVisitor.visit(ctx.expr());
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        Object assignedObject = resolvableExpression.resolve(bindings);
        String variableIdentifier = ctx.varID().getText();
        bindings.put(variableIdentifier, assignedObject);
        return assignedObject;
    }

    private String getName(VtlParser.ConstantContext ctx) {
        String text = ctx.getText();
        if (text.startsWith("\"") && text.endsWith("\"")) {
            text = text.substring(1, text.length() - 1);
        }
        return text;
    }

    @Override
    public Object visitDefDatapointRuleset(VtlParser.DefDatapointRulesetContext ctx) {
        String name = ctx.rulesetID().getText();
        // TODO: handle alias better
        List<String> variables = ctx.rulesetSignature().signature()
                .stream()
                .map(s -> s.alias() != null ? s.alias().getText() : s.getText())
                .collect(Collectors.toList());
        List<DataPointRule> rules = ctx.ruleClauseDatapoint().ruleItemDatapoint()
                .stream()
                .map(c -> {
                    VtlParser.ExprContext antecedentCondition = c.antecedentContiditon;
                    VtlParser.ExprContext consequentCondition = c.consequentCondition;
                    Function<Map<String, Object>, ExpressionVisitor> getExpressionVisitor = m -> new ExpressionVisitor(m, processingEngine, engine);
                    Function<Map<String, Object>, ResolvableExpression> buildAntecedentExpression = m -> getExpressionVisitor.apply(m).visit(antecedentCondition);
                    Function<Map<String, Object>, ResolvableExpression> buildConsequentExpression = m -> getExpressionVisitor.apply(m).visit(consequentCondition);

                    String errorCode = c.erCode() != null ? getName(c.erCode().constant()) : null;
                    String errorLevel = c.erLevel() != null ? getName(c.erLevel().constant()) : null;
                    DataPointRule dataPointRule = new DataPointRule(
                            buildAntecedentExpression,
                            buildConsequentExpression,
                            errorCode,
                            errorLevel
                    );
                    return dataPointRule;
                })
                .collect(Collectors.toList());
        DataPointRuleset dataPointRuleset = new DataPointRuleset(name, rules, variables);
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.put(name, dataPointRuleset);
        return dataPointRuleset;
    }
}

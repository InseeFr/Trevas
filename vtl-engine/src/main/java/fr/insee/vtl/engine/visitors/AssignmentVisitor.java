package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.DataPointRule;
import fr.insee.vtl.model.DataPointRuleset;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.script.Bindings;
import javax.script.ScriptContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
        String rulesetName = ctx.rulesetID().getText();
        List<VtlParser.SignatureContext> signature = ctx.rulesetSignature().signature();
        List<String> variables = signature.stream()
                .map(s -> s.varID().getText())
                .collect(Collectors.toList());
        Map<String, String> alias = signature.stream()
                .filter(s -> null != s.alias())
                .collect(Collectors.toMap(k -> k.varID().getText(), v -> v.alias().getText()));

        Set<Class> erCodeTypes = ctx.ruleClauseDatapoint().ruleItemDatapoint().stream().map(c -> {
            VtlParser.ErCodeContext erCodeContext = c.erCode();
            if (null == erCodeContext) return Object.class;
            return expressionVisitor.visit(c.erCode()).getType();
        }).collect(Collectors.toSet());
        List<Class> filteredErCodeTypes = erCodeTypes.stream().filter(t -> !t.equals(Object.class)).collect(Collectors.toList());
        if (filteredErCodeTypes.size() > 1) {
            throw new VtlRuntimeException(
                    new InvalidArgumentException("Error codes of rules have different types", ctx)
            );
        }
        Class erCodeType = filteredErCodeTypes.isEmpty() ? String.class : filteredErCodeTypes.iterator().next();

        Set<Class> erLevelTypes = ctx.ruleClauseDatapoint().ruleItemDatapoint().stream().map(c -> {
            VtlParser.ErLevelContext erLevelContext = c.erLevel();
            if (null == erLevelContext) return Object.class;
            return expressionVisitor.visit(c.erLevel()).getType();
        }).collect(Collectors.toSet());
        List<Class> filteredErLevelTypes = erLevelTypes.stream().filter(t -> !t.equals(Object.class)).collect(Collectors.toList());
        if (filteredErLevelTypes.size() > 1) {
            throw new VtlRuntimeException(
                    new InvalidArgumentException("Error levels of rules have different types", ctx)
            );
        }
        Class erLevelType = filteredErLevelTypes.isEmpty() ? Long.class : filteredErLevelTypes.iterator().next();

        AtomicInteger index = new AtomicInteger();
        List<DataPointRule> rules = ctx.ruleClauseDatapoint().ruleItemDatapoint()
                .stream()
                .map(c -> {
                    TerminalNode identifier = c.IDENTIFIER();
                    String name = null != identifier ? identifier.getText() : rulesetName + "_" + index;
                    VtlParser.ExprContext antecedentCondition = c.antecedentContiditon;
                    VtlParser.ExprContext consequentCondition = c.consequentCondition;
                    Function<Map<String, Object>, ExpressionVisitor> getExpressionVisitor = m -> new ExpressionVisitor(m, processingEngine, engine);
                    Function<Map<String, Object>, ResolvableExpression> buildAntecedentExpression = m -> getExpressionVisitor.apply(m).visit(antecedentCondition);
                    Function<Map<String, Object>, ResolvableExpression> buildConsequentExpression = m -> getExpressionVisitor.apply(m).visit(consequentCondition);
                    ResolvableExpression errorCodeExpression = null != c.erCode() ? expressionVisitor.visit(c.erCode()) : null;
                    ResolvableExpression errorLevelExpression = null != c.erLevel() ? expressionVisitor.visit(c.erLevel()) : null;
                    return new DataPointRule(
                            name,
                            buildAntecedentExpression,
                            buildConsequentExpression,
                            errorCodeExpression,
                            errorLevelExpression
                    );
                })
                .collect(Collectors.toList());
        DataPointRuleset dataPointRuleset = new DataPointRuleset(
                rulesetName,
                rules,
                variables,
                alias,
                erCodeType,
                erLevelType);
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.put(rulesetName, dataPointRuleset);
        return dataPointRuleset;
    }
}

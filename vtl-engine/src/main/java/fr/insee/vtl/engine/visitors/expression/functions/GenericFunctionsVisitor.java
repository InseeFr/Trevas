package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.FunctionNotFoundException;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.CastExpression;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.engine.expressions.DatasetFunctionExpression;
import fr.insee.vtl.engine.expressions.FunctionExpression;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>GenericFunctionsVisitor</code> is the base visitor for cast expressions.
 */
public class GenericFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final VtlScriptEngine engine;
    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     * @param engine            The {@link VtlScriptEngine}.
     */
    public GenericFunctionsVisitor(ExpressionVisitor expressionVisitor, VtlScriptEngine engine) {
        this.engine = Objects.requireNonNull(engine);
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Method to map basic scalar types and classes.
     *
     * @param basicScalarType Basic scalar type.
     * @param basicScalarText Basic scalar text.
     */
    private static Class<?> getOutputClass(Integer basicScalarType, String basicScalarText) {
        switch (basicScalarType) {
            case VtlParser.STRING:
                return String.class;
            case VtlParser.INTEGER:
                return Long.class;
            case VtlParser.NUMBER:
                return Double.class;
            case VtlParser.BOOLEAN:
                return Boolean.class;
            case VtlParser.DATE:
                return Instant.class;
            default:
                throw new UnsupportedOperationException("basic scalar type " + basicScalarText + " unsupported");
        }
    }

    public ResolvableExpression invokeFunction(String funcName, List<ResolvableExpression> parameters, Positioned position) throws VtlScriptException {
        // TODO: Use parameters to find functions so we can override them.
        var method = engine.findMethod(funcName);
        if (method.isEmpty()) {
            throw new FunctionNotFoundException(funcName, position);
        }
        var parameterTypes = parameters.stream().map(ResolvableExpression::getType).collect(Collectors.toList());
        // TODO: Method with more that one dataset parameters.
        //          ie: parameterTypes.contains(Dataset.class)

        if (parameterTypes.stream().anyMatch(clazz -> clazz.equals(Dataset.class))) {
            // If all the datasets only contain one measure use it as the parameter:
            // fn(arg1<type1>, arg2<type2>, ..., argN<typeN>)
            // res := fn(ds1#m1<type1>, ds2#arg2<type2>, ..., dsN#argN<typeN>)
            // is equivalent to:
            // tmp := inner_join(ds1, ds2, ..., dsN)
            // tmp := tmp[calc res := fn(ds1#m1<type1>, ds2#arg2<type2>, ..., dsN#argN<typeN>)
            // res := tmp[keep res]

            ProcessingEngine proc = engine.getProcessingEngine();

            Map<String, DatasetExpression> dsToJoin = new LinkedHashMap<>();
            List<ResolvableExpression> componentParams = new ArrayList<>();
            var expectedParameter = Arrays.asList(method.get().getParameterTypes()).iterator();
            for (ResolvableExpression parameter : parameters) {
                var expectedType = expectedParameter.next();
                if (parameter instanceof DatasetExpression) {
                    var dsExpr = (DatasetExpression) parameter;
                    // Check that the dataset is mono measure and of the correct type.
                    var measures = dsExpr.getDataStructure().values().stream()
                            .filter(Structured.Component::isMeasure)
                            .collect(Collectors.toList());
                    if (measures.size() != 1) {
                        throw new InvalidArgumentException("only support mono-measure dataset (ds#measure)", parameter);
                    }
                    Structured.Component component = measures.get(0);
                    if (!expectedType.isAssignableFrom(component.getType())) {
                        throw new InvalidTypeException(expectedType, component.getType(), parameter);
                    }
                    dsToJoin.put(
                            parameter.toString(),
                            proc.executeRename(dsExpr, Map.of(component.getName(), parameter.toString()))
                    );
                    componentParams.add(new ComponentExpression(
                            new Structured.Component(parameter.toString(), component.getType(), Dataset.Role.MEASURE), parameter)
                    );
                } else {
                    componentParams.add(parameter);
                }

            }
            var identifiers = JoinFunctionsVisitor.checkSameIdentifiers(dsToJoin.values()).orElseThrow(() -> new VtlRuntimeException(
                    new InvalidArgumentException("datasets must have common identifiers", position)
            ));
            var tmpDs = proc.executeInnerJoin(renameDuplicates(identifiers, dsToJoin), identifiers);
            tmpDs = proc.executeCalc(
                    tmpDs,
                    Map.of("res", new FunctionExpression(method.get(), componentParams, position)),
                    Map.of("res", Dataset.Role.MEASURE),
                    Map.of()
            );
            return proc.executeProject(tmpDs, List.of("id", "res"));
        }

        if (parameterTypes.size() == 1 && parameterTypes.get(0).equals(Dataset.class)) {
            return new DatasetFunctionExpression(method.get(), (DatasetExpression) parameters.get(0), position);
        } else {
            return new FunctionExpression(method.get(), parameters, position);
        }
    }

    // TODO: This is copied from JoinFunctionVisitor.
    private Map<String, DatasetExpression> renameDuplicates(List<Structured.Component> identifiers,
                                                            Map<String, DatasetExpression> datasets) {
        Set<String> identifierNames = identifiers.stream().map(Structured.Component::getName).collect(Collectors.toSet());
        Set<String> duplicates = new LinkedHashSet<>();
        Set<String> uniques = new LinkedHashSet<>();
        for (DatasetExpression dataset : datasets.values()) {
            for (String name : dataset.getColumnNames()) {
                // Ignore identifiers.
                if (identifierNames.contains(name)) {
                    continue;
                }
                // Compute duplicates.
                if (!uniques.add(name)) {
                    duplicates.add(name);
                }
            }
        }

        // Use duplicates to rename columns
        Map<String, DatasetExpression> result = new LinkedHashMap<>();
        for (Map.Entry<String, DatasetExpression> entry : datasets.entrySet()) {
            var name = entry.getKey();
            var dataset = entry.getValue();
            Map<String, String> fromTo = new LinkedHashMap<>();
            for (String columnName : dataset.getColumnNames()) {
                if (duplicates.contains(columnName)) {
                    fromTo.put(columnName, name + "#" + columnName);
                }
            }
            result.put(name, this.engine.getProcessingEngine().executeRename(dataset, fromTo));
        }

        return result;
    }


    @Override
    public ResolvableExpression visitCallDataset(VtlParser.CallDatasetContext ctx) {
        // Strange name, this is the generic function syntax; fnName ( param, * ).
        try {
            List<ResolvableExpression> parameters = ctx.parameter().stream().
                    map(exprVisitor::visit)
                    .collect(Collectors.toList());
            return invokeFunction(ctx.operatorID().getText(), parameters, fromContext(ctx));
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }

    /**
     * Visits expressions with cast operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the cast operation.
     */
    @Override
    public ResolvableExpression visitCastExprDataset(VtlParser.CastExprDatasetContext ctx) {
        ResolvableExpression expression = exprVisitor.visit(ctx.expr());
        TerminalNode maskNode = ctx.STRING_CONSTANT();
        // STRING_CONSTANT().getText return null or a string wrapped by quotes
        String mask = maskNode == null
                ? null
                : maskNode.getText().replaceAll("\"", "").replace("YYYY", "yyyy").replace("DD", "dd");
        Token symbol = ((TerminalNode) ctx.basicScalarType().getChild(0)).getSymbol();
        Integer basicScalarType = symbol.getType();
        String basicScalarText = symbol.getText();

        Class<?> outputClass = getOutputClass(basicScalarType, basicScalarText);

        if (Object.class.equals(expression.getType())) {
            return ResolvableExpression.withType(Object.class).withPosition(fromContext(ctx)).using(c -> null);
        }
        try {
            return new CastExpression(fromContext(ctx), expression, mask, outputClass);
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }
}

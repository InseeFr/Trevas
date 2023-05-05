package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.FunctionNotFoundException;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.CastExpression;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.engine.expressions.FunctionExpression;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.TypedExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public List<DatasetExpression> splitToMonoMeasure(DatasetExpression dataset) {
        ProcessingEngine proc = engine.getProcessingEngine();
        List<Structured.Component> identifiers = dataset.getIdentifiers();
        return dataset.getMeasures().stream().map(measure -> {
            List<String> idAndMeasure = Stream.concat(identifiers.stream(), Stream.of(measure))
                    .map(Structured.Component::getName)
                    .collect(Collectors.toList());
            return proc.executeProject(dataset, idAndMeasure);
        }).collect(Collectors.toList());
    }

    public ResolvableExpression invokeFunction(String funcName, List<ResolvableExpression> parameters, Positioned position) throws VtlScriptException {
        try {
            List<DatasetExpression> noMonoDs = parameters.stream().filter(e -> e instanceof DatasetExpression && !(((DatasetExpression) e).isMonoMeasure()))
                    .map(ds -> (DatasetExpression) ds)
                    .collect(Collectors.toList());
            if (noMonoDs.size() > 2) {
                throw new VtlRuntimeException(
                        new InvalidArgumentException("too many no mono-measure datasets (" + noMonoDs.size() + ")", position)
                );
            }

            ProcessingEngine proc = engine.getProcessingEngine();
//            TODO
//            if (noMonoDs has not same shape) throw

            // Invoking a function only supports a combination of scalar types and mono-measure arrays. In the special
            // case of bi-functions (a + b or f(a,b)) the two datasets must have the same identifiers and measures.
            ResolvableExpression finalRes;
            // Only one parameter, and it's a dataset. We can invoke the function on each measure.
            if (!parameters.stream().anyMatch(e -> e instanceof DatasetExpression)) {
                // Only scalar types. We can invoke the function directly.
                var method = engine.findMethod(funcName, parameters.stream().map(ResolvableExpression::getType).collect(Collectors.toList()));
                return new FunctionExpression(method, parameters, position);
            } else if (noMonoDs.size() == 0) {
                finalRes = invokeFunctionOnDataset(funcName, parameters, position);
            } else {
                List<Structured.Component> measures = noMonoDs.get(0).getDataStructure().getMeasures();
                Map<String, DatasetExpression> results = new HashMap();
                for (Structured.Component measure : measures) {
                    List<ResolvableExpression> params = parameters.stream().map(p -> {
                        if (p instanceof DatasetExpression) {
                            DatasetExpression ds = (DatasetExpression) p;
                            List<String> idAndMeasure = Stream.concat(ds.getIdentifiers().stream(), Stream.of(measure))
                                    .map(Structured.Component::getName)
                                    .collect(Collectors.toList());
                            return proc.executeProject(ds, idAndMeasure);
                        } else return p;
                    }).collect(Collectors.toList());
                    results.put(measure.getName(), invokeFunctionOnDataset(funcName, params, position));
                }
                finalRes = proc.executeInnerJoin(results);
            }
            if (finalRes instanceof DatasetExpression) {
                List<Structured.Component> measures = ((DatasetExpression) finalRes).getMeasures();
                if (measures.size() == 1 && measures.get(0).getType().equals(Boolean.class)) {
                    // TODO: refine with constraints matrix
                    return proc.executeRename((DatasetExpression) finalRes, Map.of(measures.get(0).getName(), "bool_var"));
                }
            }
            return finalRes;
        } catch (NoSuchMethodException e) {
            throw new VtlRuntimeException(new FunctionNotFoundException(e.getMessage(), position));
        }
    }

    private DatasetExpression invokeFunctionOnDataset(String funcName, List<ResolvableExpression> parameters, Positioned position) throws NoSuchMethodException, VtlScriptException {
        ProcessingEngine proc = engine.getProcessingEngine();

        // Normalize all parameters to datasets first.
        // 1. Join all the datasets together and build a new expression map.
        Map<String, ResolvableExpression> monoExprs = new HashMap<>();
        Set<String> measureNames = new HashSet();
        var dsExprs = parameters.stream()
                .filter(e -> e instanceof DatasetExpression)
                .map(e -> ((DatasetExpression) e))
                .map(ds -> {
                    if (!ds.isMonoMeasure()) {
                        throw new VtlRuntimeException(new InvalidArgumentException("mono-measure dataset expected", ds));
                    }
                    var uniqueName = "arg" + ds.hashCode();
                    var measure = ds.getMeasures().get(0);
                    String measureName = measure.getName();
                    measureNames.add(measureName);
                    ds = proc.executeRename(ds, Map.of(measureName, uniqueName));
                    var renamedComponent = new Structured.Component(uniqueName, measure.getType(), measure.getRole(), measure.getNullable());
                    monoExprs.put(uniqueName, new ComponentExpression(renamedComponent, ds));
                    return ds;
                })
                .collect(Collectors.toMap(e -> "arg" + e.hashCode(), e -> e));
        if (measureNames.size() != 1) {
            throw new VtlRuntimeException(
                    new InvalidArgumentException("mono-measure datasets don't contain same measures (number or names)", position)
            );
        }
        DatasetExpression ds = proc.executeInnerJoin(dsExprs);

        // Rebuild the function parameters. TODO: All component?
        var normalizedParams = parameters.stream()
                .map(e -> monoExprs.getOrDefault("arg" + e.hashCode(), e))
                .collect(Collectors.toList());

        // 3. Invoke the function.
        Method method = engine.findMethod(funcName, normalizedParams.stream()
                .map(TypedExpression::getType)
                .collect(Collectors.toList()));
        var funcExrp = new FunctionExpression(method, normalizedParams, position);
        ds = proc.executeCalc(ds, Map.of("result", funcExrp), Map.of("result", Dataset.Role.MEASURE), Map.of());
        ds = proc.executeProject(ds, Stream.concat(ds.getIdentifiers().stream().map(Structured.Component::getName), Stream.of("result")).collect(Collectors.toList()));
        return proc.executeRename(ds, Map.of("result", measureNames.iterator().next()));
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
            // replace because of spark issue with dot inside col getter arg
            var name = entry.getKey().replace(".", "");
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
            return ResolvableExpression.withType(outputClass).withPosition(fromContext(ctx)).using(c -> null);
        }
        try {
            return new CastExpression(fromContext(ctx), expression, mask, outputClass);
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }
}

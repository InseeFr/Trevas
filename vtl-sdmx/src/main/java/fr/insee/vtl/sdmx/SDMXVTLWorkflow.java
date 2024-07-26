package fr.insee.vtl.sdmx;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.PersistentDataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.utils.Java8Helpers;
import io.sdmx.api.io.ReadableDataLocation;
import io.sdmx.api.sdmx.model.beans.SdmxBeans;
import io.sdmx.api.sdmx.model.beans.base.INamedBean;
import io.sdmx.api.sdmx.model.beans.transformation.IRulesetSchemeBean;
import io.sdmx.api.sdmx.model.beans.transformation.ITransformationBean;
import io.sdmx.api.sdmx.model.beans.transformation.ITransformationSchemeBean;
import io.sdmx.format.ml.api.engine.StaxStructureReaderEngine;
import io.sdmx.format.ml.engine.structure.reader.v3.StaxStructureReaderEngineV3;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;
import java.util.stream.Collectors;

public class SDMXVTLWorkflow {

    final ScriptEngine engine;
    final SdmxBeans sdmxBeans;
    private static final StaxStructureReaderEngine readerEngineSDMX3 = StaxStructureReaderEngineV3.getInstance();

    final Map<String, Dataset> inputs;


    public SDMXVTLWorkflow(ScriptEngine engine, ReadableDataLocation rdl, Map<String, Dataset> inputs) {
        this.engine = engine;
        this.sdmxBeans = readerEngineSDMX3.getSdmxBeans(rdl);
        this.inputs = inputs;
    }

    // Adds semicolon if no present.
    private static String addSemi(String code) {
        if (code.endsWith(";")) {
            return code;
        }
        return code + ";";
    }

    private static String toComment(String comment) {
        if (comment == null || comment.trim().isEmpty()) {
            return "";
        }
        return Arrays.stream(comment.split("\n"))
                .map(s -> "// " + s).collect(Collectors.joining("\n")) + "\n";
    }

    private Map<String, String> getRulesets() {
        Set<IRulesetSchemeBean> vtlRulesetSchemeBean = sdmxBeans.getVtlRulesetSchemeBean();
        return vtlRulesetSchemeBean.stream().filter(
                        v -> v.getVtlVersion().equals(engine.getFactory().getLanguageVersion())
                )
                .flatMap(v -> v.getItems().stream())
                .collect(Collectors.toMap(INamedBean::getId, v -> {
                                    String desc = toComment(v.getDescription());
                                    String definition = addSemi(v.getRulesetDefinition());
                                    return desc + definition;
                                },
                                (u, v) -> {
                                    throw new IllegalStateException(String.format("Duplicate key %s", u));
                                },
                                LinkedHashMap::new)
                );
    }

    public String getRulesetsVTL() {
        return String.join("\n\n", getRulesets().values());
    }

    public Map<String, String> getTransformations() {
        Set<ITransformationSchemeBean> vtlTransformationSchemeBean = sdmxBeans.getVtlTransformationSchemeBean();
        return vtlTransformationSchemeBean.stream().filter(
                        v -> v.getVtlVersion().equals(engine.getFactory().getLanguageVersion())
                ).flatMap(
                        v -> v.getItems().stream()
                )
                .collect(Collectors.toMap(ITransformationBean::getResult, v -> {
                            String varName = v.getResult();
                            String comment = toComment(v.getDescription());
                            String expression = addSemi(v.getExpression());
                            String assignment = (v.isPersistent() ? " <- " : " := ");
                            return comment + varName + assignment + expression;
                        },
                        (u, v) -> {
                            throw new IllegalStateException(String.format("Duplicate key %s", u));
                        },
                        LinkedHashMap::new
                ));
    }

    public String getTransformationsVTL() {
        return String.join("\n\n", getTransformations().values());
    }

    public Map<String, Dataset> getEmptyDatasets() {
        return TrevasSDMXUtils.parseDataStructure(sdmxBeans).entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<>(
                        e.getKey(),
                        new InMemoryDataset(Java8Helpers.listOf(), (Map<String, Structured.Component>) e.getValue())
                )).collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (u, v) -> {
                            throw new IllegalStateException(String.format("Duplicate key %s", u));
                        },
                        LinkedHashMap::new
                ));
    }

    public Map<String, PersistentDataset> run() {
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.putAll(inputs);
        Map<String, String> rulesets = getRulesets();
        rulesets.forEach((k, v) -> {
            try {
                engine.eval(v);
            } catch (ScriptException e) {
                throw new RuntimeException("Invalid ruleset definition for:" + k, e);
            }
        });
        Map<String, String> transformations = getTransformations();
        transformations.forEach((k, v) -> {
            try {
                engine.eval(v);
            } catch (ScriptException e) {
                throw new RuntimeException("Invalid step definition for:" + k, e);
            }
        });
        return bindings.entrySet().stream().filter(p -> p.getValue() instanceof PersistentDataset)
                .collect(Collectors.toMap(Map.Entry::getKey, p -> (PersistentDataset) p.getValue()));
    }
}

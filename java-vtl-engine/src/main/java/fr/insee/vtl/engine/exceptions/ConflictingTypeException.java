package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;
import java.util.stream.Collectors;

public class ConflictingTypeException extends VtlScriptException {
    public ConflictingTypeException(ParseTree tree, List<Class<?>> types) {
        super(String.format("conflicting types: %s", types.stream().map(Class::getSimpleName)
                .collect(Collectors.toList())), tree);
    }
}

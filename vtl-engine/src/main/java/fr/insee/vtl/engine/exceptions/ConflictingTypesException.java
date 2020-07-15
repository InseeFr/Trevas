package fr.insee.vtl.engine.exceptions;

import java.util.Collection;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTree;

public class ConflictingTypesException extends VtlScriptException {
    public ConflictingTypesException(ParseTree tree, Collection<Class<?>> types) {
        super(String.format("conflicting types: %s", types.stream().map(Class::getSimpleName)
                .collect(Collectors.toList())), tree);
    }
}

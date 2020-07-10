package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

public class UnsupportedTypeException extends VtlScriptException {
    public UnsupportedTypeException(ParseTree tree, Class<?> unsupportedType) {
        super(String.format("the type %s is not supported", unsupportedType), tree);
    }
}

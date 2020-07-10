package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

public class UndefinedVariableException extends VtlScriptException {

    public UndefinedVariableException(ParseTree tree) {
        super(String.format("undefined variable %s", tree.getText()), tree);
    }

}

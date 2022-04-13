package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

public class UnimplementedException extends VtlScriptException {

    public UnimplementedException(String msg, ParseTree tree) {
        super(msg, tree);
    }
}

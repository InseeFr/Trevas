package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

import javax.script.ScriptException;

// TODO: Implement file, line & getters.
public class VtlScriptException extends ScriptException {

    private final ParseTree tree;

    public VtlScriptException(String msg, ParseTree tree) {
        super(msg);
        this.tree = tree;
    }

    public VtlScriptException(Exception e, ParseTree tree) {
        super(e);
        this.tree = tree;
    }

    public ParseTree getTree() {
        return tree;
    }

}

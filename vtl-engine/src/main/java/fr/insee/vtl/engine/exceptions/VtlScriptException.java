package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

import javax.script.ScriptException;

/**
 * The <code>VtlScriptException</code> is the base class for all VTL exceptions.
 */
// TODO: Implement file, line & getters.
public class VtlScriptException extends ScriptException {

    private final ParseTree tree;

    /**
     * Constructor taking the exception message and the parsing context.
     *
     * @param msg The message for the exception.
     * @param tree The parsing context where the exception is thrown.
     */
    public VtlScriptException(String msg, ParseTree tree) {
        super(msg);
        this.tree = tree;
    }

    /**
     * Constructor taking the mother exception and the parsing context.
     *
     * @param e The mother exception.
     * @param tree The parsing context where the exception is thrown.
     */
    public VtlScriptException(Exception e, ParseTree tree) {
        super(e);
        this.tree = tree;
    }

    /**
     * Returns the parsing context where the exception was raised.
     *
     * @return The parsing context where the exception was raised.
     */
    public ParseTree getTree() {
        return tree;
    }
}

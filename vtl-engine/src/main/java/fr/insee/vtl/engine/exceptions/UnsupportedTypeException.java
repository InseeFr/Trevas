package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

/**
 * The <code>UnsupportedTypeException</code> indicates that an element used in an expression has a type which is not supported.
 */
public class UnsupportedTypeException extends VtlScriptException {

    /**
     * Constructor taking the parsing context and the type which is not supported.
     *
     * @param unsupportedType The type which is not supported.
     * @param tree The parsing context where the exception is thrown.
     */
    public UnsupportedTypeException(Class<?> unsupportedType, ParseTree tree) {
        super(String.format("the type %s is not supported", unsupportedType), tree);
    }
}

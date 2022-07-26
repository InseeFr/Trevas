package fr.insee.vtl.model;

import javax.script.ScriptEngine;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * The function provider is used to register new function to be made available in
 * the VTLEngine.
 */
public interface FunctionProvider {

    /**
     * Return a map of functions to add to the VTL engine.
     *
     * @param vtlEngine the VTL implementation of the {@link ScriptEngine}.
     * @return a map of function name and {@link Method}.
     */
    Map<String, Method> getFunctions(ScriptEngine vtlEngine);
}

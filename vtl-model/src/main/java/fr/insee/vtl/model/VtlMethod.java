package fr.insee.vtl.model;

import fr.insee.vtl.model.exceptions.VtlScriptException;

import java.io.Serializable;
import java.lang.reflect.Method;

public class VtlMethod implements Serializable {

    String className;
    String methodName;

    public VtlMethod(Method method) {
        className = method.getDeclaringClass().getName();
        methodName = method.getName();
    }

    public Method getMethod(Positioned pos, Class<?>... parameterTypes) throws VtlScriptException {
        try {
            return Class.forName(className).getMethod(methodName, parameterTypes);
        } catch (Exception e) {
            throw new VtlScriptException("could not deserialize method " + methodName, pos);
        }
    }
}

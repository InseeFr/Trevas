package fr.insee.vtl.model;

import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.io.Serializable;
import java.lang.reflect.Method;

public class VtlMethod implements Serializable {

  private final Class<?>[] types;
  private final String className;
  private final String methodName;

  public VtlMethod(Method method) {
    className = method.getDeclaringClass().getName();
    methodName = method.getName();
    types = method.getParameterTypes();
  }

  public Method getMethod(Positioned pos) throws VtlScriptException {
    try {
      return Class.forName(className).getMethod(methodName, this.types);
    } catch (Exception e) {
      throw new VtlScriptException(
          "could not deserialize method " + methodName + ": " + e.getMessage(), pos);
    }
  }
}

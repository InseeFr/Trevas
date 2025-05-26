package fr.insee.vtl.engine;

import java.util.List;
import java.util.Objects;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;

/**
 * <code>VtlScriptEngineFactory</code> is used to instantiate VTL script engines and hold their
 * basic description.
 */
public class VtlScriptEngineFactory implements ScriptEngineFactory {

  /**
   * Returns the full name of the VTL script engine.
   *
   * @return The full name of the VTL script engine.
   */
  @Override
  public String getEngineName() {
    return "Trevas VTL engine";
  }

  /**
   * Returns the version number of the VTL script engine.
   *
   * @return The version number of the VTL script engine.
   */
  @Override
  public String getEngineVersion() {
    return getClass().getPackage().getImplementationVersion();
  }

  /**
   * Returns a list of filename extensions associated to VTL scripts.
   *
   * @return The list of filename extensions associated to VTL scripts.
   */
  @Override
  public List<String> getExtensions() {
    return List.of("vtl");
  }

  /**
   * Returns a list of MIME types corresponding to VTL scripts.
   *
   * @return The list of MIME types corresponding to VTL scripts (empty list for now).
   */
  @Override
  public List<String> getMimeTypes() {
    return List.of();
  }

  /**
   * Returns a list of short names for the VTL script engine.
   *
   * @return A list of short names for the VTL script engine.
   */
  @Override
  public List<String> getNames() {
    return List.of(getLanguageName(), getEngineName(), "vtl", "Trevas", "trevas");
  }

  /**
   * Returns the name of the scripting language supported by the engine.
   *
   * @return The name of the scripting language supported by the engine ("VTL").
   */
  @Override
  public String getLanguageName() {
    return "VTL";
  }

  /**
   * Returns the version of the scripting language supported by the engine.
   *
   * @return The version of the scripting language supported by the engine ("2.0").
   */
  @Override
  public String getLanguageVersion() {
    return "2.0";
  }

  /**
   * The <code>getParameter</code> method is not supported by this engine factory.
   *
   * @throws An empty <code>UnsupportedOperationException</code>.
   */
  @Override
  public Object getParameter(String key) {
    Objects.requireNonNull(key);
    return null;
  }

  /**
   * The <code>getMethodCallSyntax</code> method is not supported by this engine factory.
   *
   * @throws An empty <code>UnsupportedOperationException</code>.
   */
  @Override
  public String getMethodCallSyntax(String obj, String m, String... args) {
    throw new UnsupportedOperationException();
  }

  /**
   * The <code>getOutputStatement</code> method is not supported by this engine factory.
   *
   * @throws An empty <code>UnsupportedOperationException</code>.
   */
  @Override
  public String getOutputStatement(String toDisplay) {
    throw new UnsupportedOperationException();
  }

  /**
   * The <code>getProgram</code> method is not supported by this engine factory.
   *
   * @throws An empty <code>UnsupportedOperationException</code>.
   */
  @Override
  public String getProgram(String... statements) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns a VTL script engine instance associated with this factory.
   *
   * @return A new <code>VtlScriptEngine</code> instance.
   */
  @Override
  public ScriptEngine getScriptEngine() {
    return new VtlScriptEngine(this);
  }
}

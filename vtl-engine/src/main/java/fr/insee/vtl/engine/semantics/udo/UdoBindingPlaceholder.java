package fr.insee.vtl.engine.semantics.udo;

/** Compile-time placeholder so {@link UdoTypeInference} can visit a UDO body with typed formals. */
public final class UdoBindingPlaceholder {

  private final Class<?> type;

  public UdoBindingPlaceholder(Class<?> type) {
    this.type = type;
  }

  public Class<?> type() {
    return type;
  }
}

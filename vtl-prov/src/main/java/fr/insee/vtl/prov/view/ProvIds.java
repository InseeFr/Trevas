package fr.insee.vtl.prov.view;

/** Shared parsing of versioned IR ids ({@code name@version}, {@code ds.comp}). */
final class ProvIds {

  private ProvIds() {}

  static String bindingName(String versionedId) {
    int at = versionedId.lastIndexOf('@');
    return at < 0 ? versionedId : versionedId.substring(0, at);
  }

  static String componentName(String varId) {
    int dot = varId.lastIndexOf('.');
    return dot < 0 ? varId : varId.substring(dot + 1);
  }
}

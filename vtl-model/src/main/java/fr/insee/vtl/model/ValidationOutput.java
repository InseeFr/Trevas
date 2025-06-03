package fr.insee.vtl.model;

public enum ValidationOutput {
  ALL("all"),
  ALL_MEASURES("all_measures"),
  INVALID("invalid");

  public final String value;

  ValidationOutput(String value) {
    this.value = value;
  }

  public static boolean contains(String value) {
    for (ValidationOutput vo : ValidationOutput.values()) {
      if (vo.value.equals(value)) {
        return true;
      }
    }

    return false;
  }
}

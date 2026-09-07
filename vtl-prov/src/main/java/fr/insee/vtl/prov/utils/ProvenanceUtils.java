package fr.insee.vtl.prov.utils;

import java.util.UUID;

public class ProvenanceUtils {

  public static String generateUUID() {
    return UUID.randomUUID().toString();
  }
}

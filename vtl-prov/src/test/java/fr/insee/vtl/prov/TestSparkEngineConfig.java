package fr.insee.vtl.prov;

final class TestSparkEngineConfig {

  private static final String ENGINE_PROPERTY = "trevas.spark.processing.engine";
  private static final String DEFAULT_ENGINE = "spark";

  private TestSparkEngineConfig() {}

  static String getEngineName() {
    return System.getProperty(ENGINE_PROPERTY, DEFAULT_ENGINE);
  }
}

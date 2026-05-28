package fr.insee.vtl.prov;

import org.apache.spark.sql.SparkSession;

final class TestSparkSessionFactory {

  private TestSparkSessionFactory() {}

  static SparkSession create() {
    return SparkSession.builder()
        .appName("test")
        .master("local")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate();
  }
}

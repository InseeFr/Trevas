package fr.insee.vtl.spark4;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.spark.SparkDataset;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SparkDatasetMaterializationTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .appName("materialization-test")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() throws IOException {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  void columnBindingsMapCollectsOnlyKeyAndValueColumns() {
    StructType schema =
        DataTypes.createStructType(
            List.of(
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("measure", DataTypes.DoubleType, false),
                DataTypes.createStructField("noise", DataTypes.StringType, false)));
    Dataset<Row> frame =
        spark.createDataFrame(
            List.of(
                RowFactory.create("A", 1.0D, "x"),
                RowFactory.create("B", 2.0D, "y"),
                RowFactory.create("A", 3.0D, "z")),
            schema);

    Map<String, Object> bindings = SparkDataset.columnBindingsMap(frame, "id", "measure");

    assertThat(bindings).containsEntry("A", 3.0D).containsEntry("B", 2.0D);
  }

  @Test
  void wrappingSparkDatasetReusesUnderlyingDataFrame() {
    StructType schema =
        DataTypes.createStructType(
            List.of(DataTypes.createStructField("id", DataTypes.StringType, false)));
    Dataset<Row> frame = spark.createDataFrame(List.of(RowFactory.create("x")), schema);
    SparkDataset original = new SparkDataset(frame);

    SparkDataset wrapped =
        new SparkDataset(original, Map.of("id", fr.insee.vtl.model.Dataset.Role.IDENTIFIER), spark);

    assertThat(wrapped.getSparkDataset()).isSameAs(original.getSparkDataset());
  }

  @Test
  void inMemoryDatasetStillConvertsThroughDriver() {
    InMemoryDataset memory =
        new InMemoryDataset(
            List.of(List.of("id-1", 42L)),
            List.of(
                new fr.insee.vtl.model.Structured.Component(
                    "id", String.class, fr.insee.vtl.model.Dataset.Role.IDENTIFIER),
                new fr.insee.vtl.model.Structured.Component(
                    "m", Long.class, fr.insee.vtl.model.Dataset.Role.MEASURE)));

    SparkDataset converted =
        new SparkDataset(
            memory,
            Map.of(
                "id", fr.insee.vtl.model.Dataset.Role.IDENTIFIER,
                "m", fr.insee.vtl.model.Dataset.Role.MEASURE),
            spark);

    assertThat(converted.getSparkDataset().count()).isEqualTo(1L);
  }
}

package fr.insee.vtl.engine.attribute;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Reproduces TCK 163 viral {@code At_1} values after grouped {@code aggr}. */
class ViralAttributeGroupedAggregationTest {

  private static final Positioned TEST_POSITION = () -> new Positioned.Position("test", 1, 1, 0, 0);

  @Test
  void groupedAggrPropagatesViralAttributeValues() {
    InMemoryDataset input =
        new InMemoryDataset(
            List.of(
                row(2010L, "E", "XX", 20L, ""),
                row(2010L, "B", "XX", 1L, "H"),
                row(2010L, "R", "XX", 1L, "A"),
                row(2010L, "F", "YY", 23L, ""),
                row(2011L, "E", "XX", 20L, "P"),
                row(2011L, "B", "ZZ", 1L, "N"),
                row(2011L, "R", "YY", -1L, "P"),
                row(2011L, "F", "XX", 20L, "Z"),
                row(2012L, "L", "ZZ", 40L, "P"),
                row(2012L, "E", "YY", 30L, "P")),
            Map.of(
                "Id_1", Long.class,
                "Id_2", String.class,
                "Id_3", String.class,
                "Me_1", Long.class,
                "At_1", String.class),
            Map.of(
                "Id_1", Dataset.Role.IDENTIFIER,
                "Id_2", Dataset.Role.IDENTIFIER,
                "Id_3", Dataset.Role.IDENTIFIER,
                "Me_1", Dataset.Role.MEASURE,
                "At_1", Dataset.Role.VIRALATTRIBUTE));

    Map<String, AggregationExpression> collectors = new LinkedHashMap<>();
    collectors.put(
        "Me_2",
        AggregationExpression.max(
            ResolvableExpression.withType(Long.class)
                .withPosition(TEST_POSITION)
                .using(dp -> (Long) dp.get("Me_1"))));
    collectors.put(
        "Me_3",
        AggregationExpression.min(
            ResolvableExpression.withType(Long.class)
                .withPosition(TEST_POSITION)
                .using(dp -> (Long) dp.get("Me_1"))));

    DatasetExpression inputExpression = DatasetExpression.of(input, TEST_POSITION);
    Dataset result =
        new InMemoryProcessingEngine()
            .executeAggr(inputExpression, List.of("Id_1"), collectors)
            .resolve(Map.of());

    List<Map<String, Object>> rows = result.getDataAsMap();
    assertThat(rows).hasSize(3);
    assertThat(findRow(rows, 2010L))
        .containsEntry("Me_2", 23L)
        .containsEntry("Me_3", 1L)
        .containsEntry("At_1", "");
    assertThat(findRow(rows, 2011L))
        .containsEntry("Me_2", 20L)
        .containsEntry("Me_3", -1L)
        .containsEntry("At_1", "N");
    assertThat(findRow(rows, 2012L))
        .containsEntry("Me_2", 40L)
        .containsEntry("Me_3", 30L)
        .containsEntry("At_1", "P");
  }

  private static Map<String, Object> row(long id1, String id2, String id3, long me1, String at1) {
    return Map.of("Id_1", id1, "Id_2", id2, "Id_3", id3, "Me_1", me1, "At_1", at1);
  }

  private static Map<String, Object> findRow(List<Map<String, Object>> rows, long id1) {
    return rows.stream()
        .filter(row -> id1 == ((Number) row.get("Id_1")).longValue())
        .findFirst()
        .orElseThrow();
  }
}

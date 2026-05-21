package fr.insee.vtl.engine.attribute;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import java.util.List;
import java.util.Map;

/**
 * Multi-identifier dataset with viral {@code At_1} for grouped {@code aggr} scenarios (golden:
 * {@code ""}, {@code N}, {@code P} per {@code Id_1}).
 */
public final class GroupedAggrViralFixtures {

  private GroupedAggrViralFixtures() {}

  public static InMemoryDataset dataset() {
    return new InMemoryDataset(
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
  }

  public static void assertGroupedAggrViralValues(List<Map<String, Object>> rows) {
    org.assertj.core.api.Assertions.assertThat(rows).hasSize(3);
    org.assertj.core.api.Assertions.assertThat(findRow(rows, 2010L))
        .containsEntry("Me_2", 23L)
        .containsEntry("Me_3", 1L)
        .containsEntry("At_1", "");
    org.assertj.core.api.Assertions.assertThat(findRow(rows, 2011L))
        .containsEntry("Me_2", 20L)
        .containsEntry("Me_3", -1L)
        .containsEntry("At_1", "N");
    org.assertj.core.api.Assertions.assertThat(findRow(rows, 2012L))
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

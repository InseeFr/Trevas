package fr.insee.vtl.coverage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TckComponentRolesTest {

  @Test
  void mapsTckAliasesToTrevasRoleNames() {
    assertThat(TckComponentRoles.toTrevasRole("DIMENSION")).isEqualTo("IDENTIFIER");
    assertThat(TckComponentRoles.toTrevasRole("component")).isEqualTo("IDENTIFIER");
    assertThat(TckComponentRoles.toTrevasRole("VIRAL_ATTRIBUTE")).isEqualTo("VIRALATTRIBUTE");
  }

  @Test
  void passesThroughStandardRoleNames() {
    assertThat(TckComponentRoles.toTrevasRole("MEASURE")).isEqualTo("MEASURE");
    assertThat(TckComponentRoles.toTrevasRole("viral attribute")).isEqualTo("VIRALATTRIBUTE");
  }
}

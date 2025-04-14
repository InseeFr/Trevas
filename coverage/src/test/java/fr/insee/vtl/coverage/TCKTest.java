package fr.insee.vtl.coverage;

import org.junit.jupiter.api.Test;

import java.util.stream.Stream;



class TCKTest {
    @Test
    void runTCK() {
        TCK.runTCK();
//        assertThat(githubContent.getSubFolders().size()).isEqualTo(10);
    }

    @ParameterizedTest
    @MethodSource("testArgs")
    void checkExplicitMethodSource(String word) {

    }

    static Stream<String> testArgs() {
        return Stream.of("a1", "b2");
    }
}

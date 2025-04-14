package fr.insee.vtl.coverage;

import fr.insee.vtl.coverage.model.Folder;
import org.junit.jupiter.api.Test;

import java.util.List;


class TCKTest {
    @Test
    void runTCK() {
        List<Folder> folders = TCK.runTCK();
//        assertThat(githubContent.getSubFolders().size()).isEqualTo(10);
    }
}

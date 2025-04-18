package fr.insee.vtl.coverage;

import fr.insee.vtl.coverage.model.Folder;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;


class TCKTest {

    @TestFactory
    Stream<DynamicNode> generateTests() {
        List<Folder> tests = TCK.runTCK();
        Folder root = new Folder();
        root.setName("root");
        root.setFolders(tests);
        return Stream.of(toDynamicNode(root));
    }

    private DynamicNode toDynamicNode(Folder folder) {
        List<DynamicNode> children = new ArrayList<>();
        if (folder.getFolders() != null) {
            for (Folder sub : folder.getFolders()) {
                children.add(toDynamicNode(sub));
            }
        }

        if (folder.getTest() != null) {
            children.add(DynamicTest.dynamicTest(
                    folder.getName(),
                    () -> {
                        // TODO: Execute the test.
                        System.out.println("Executing test" + folder.getTest());
                    }
            ));
        }

        return DynamicContainer.dynamicContainer(
                folder.getName(),
                children.stream()
        );
    }
}

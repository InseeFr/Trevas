package fr.insee.vtl.coverage.tck;

import fr.insee.vtl.coverage.model.Folder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Walks the TCK folder tree and collects leaf folders that contain {@code input.json} / {@code
 * output.json} / {@code transformation.vtl}.
 */
public final class TckFolders {

  private TckFolders() {}

  /** Depth-first list of leaf cases in stable tree order. */
  public static List<TckLeafCase> collectLeaves(List<Folder> roots) {
    if (roots == null || roots.isEmpty()) {
      return List.of();
    }
    List<TckLeafCase> out = new ArrayList<>();
    for (Folder root : roots) {
      walk(root, "", out);
    }
    return List.copyOf(out);
  }

  private static void walk(Folder folder, String prefix, List<TckLeafCase> out) {
    Objects.requireNonNull(folder, "folder");
    String path = TckPaths.join(prefix, folder.getName());

    if (folder.getTest() != null) {
      out.add(new TckLeafCase(path, folder.getTest()));
      return;
    }

    List<Folder> children = folder.getFolders();
    if (children == null) {
      return;
    }
    for (Folder sub : children) {
      walk(sub, path, out);
    }
  }
}

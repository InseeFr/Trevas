package fr.insee.vtl.coverage;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.coverage.model.Folder;
import fr.insee.vtl.coverage.model.Test;
import fr.insee.vtl.coverage.utils.JSONStructureLoader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class TCK {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static List<Folder> runTCK(InputStream zipInputStream) {
    File extractedFolder;
    try {
      extractedFolder = init(zipInputStream);
    } catch (IOException e) {
      throw new RuntimeException("Error unzipping input stream", e);
    }

    try {
      return loadInput(extractedFolder);
    } catch (Exception e) {
      throw new RuntimeException("Error loading input from extracted folder", e);
    } finally {
      deleteDirectory(extractedFolder);
    }
  }

  public static List<Folder> runTCK(File zipFile) {
    try (InputStream in = Files.newInputStream(zipFile.toPath())) {
      return runTCK(in);
    } catch (IOException e) {
      throw new RuntimeException("Error reading zip file: " + zipFile, e);
    }
  }

  public static List<Folder> runTCK(String zipPath) {
    return runTCK(new File(zipPath));
  }

  private static File init(InputStream zipInputStream) throws IOException {
    Path tempDir = Files.createTempDirectory("tck-unzip-");
    try (ZipInputStream zis = new ZipInputStream(zipInputStream)) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        Path newPath = zipSlipProtect(entry, tempDir);
        if (entry.isDirectory()) {
          Files.createDirectories(newPath);
        } else {
          Files.createDirectories(newPath.getParent());
          Files.copy(zis, newPath, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
    return tempDir.toFile();
  }

  private static Path zipSlipProtect(ZipEntry entry, Path targetDir) throws IOException {
    Path target = targetDir.resolve(entry.getName()).normalize();
    if (!target.startsWith(targetDir)) {
      throw new IOException("Entry is outside of the target dir: " + entry.getName());
    }
    return target;
  }

  private static void deleteDirectory(File dir) {
    if (dir.isDirectory()) {
      for (File file : Objects.requireNonNull(dir.listFiles())) {
        deleteDirectory(file);
      }
    }
    dir.delete();
  }

  public static List<Folder> loadInput(File path) throws Exception {
    List<Folder> folders = new ArrayList<>();
    File[] files = path.listFiles();
    if (files != null) {
      boolean isTestFolder = containsTestFiles(files);

      if (isTestFolder) {
        Folder folder = new Folder();
        folder.setName(path.getName());
        Test test = new Test();

        for (File file : files) {
          switch (file.getName()) {
            case "input.json":
              test.setInput(JSONStructureLoader.loadDatasetsFromCSV(file));
              break;
            case "output.json":
              test.setOutputs(JSONStructureLoader.loadDatasetsFromCSV(file));
              break;
            case "transformation.vtl":
              String script = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
              test.setScript(script);
              break;
          }
        }
        folder.setTest(test);
        folders.add(folder);
      } else {
        for (File file : files) {
          if (file.isDirectory()) {
            Folder folder = new Folder();
            folder.setName(file.getName());
            folder.setFolders(loadInput(file));
            folders.add(folder);
          }
        }
      }
    }
    return folders;
  }

  private static boolean containsTestFiles(File[] files) {
    Set<String> required =
        new HashSet<>(Arrays.asList("input.json", "output.json", "transformation.vtl"));
    Set<String> found = new HashSet<>();
    for (File file : files) {
      if (required.contains(file.getName())) {
        found.add(file.getName());
      }
    }
    return found.containsAll(required);
  }
}

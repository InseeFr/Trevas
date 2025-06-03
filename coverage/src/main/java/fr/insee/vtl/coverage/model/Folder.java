package fr.insee.vtl.coverage.model;

import java.util.List;

public class Folder {

  private String name;
  private List<Folder> folders;
  private Test test;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Test getTest() {
    return test;
  }

  public void setTest(Test test) {
    this.test = test;
  }

  public List<Folder> getFolders() {
    return folders;
  }

  public void setFolders(List<Folder> folders) {
    this.folders = folders;
  }
}

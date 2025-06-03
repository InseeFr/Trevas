package fr.insee.vtl.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class IndexedHashMapTest {

  @Test
  public void testEquals() {
    var map1 = new IndexedHashMap<>();
    var map2 = new IndexedHashMap<>();

    map1.put("a", "1");
    map1.put("b", "2");
    map1.put("c", "3");
    map1.put("d", "4");

    map2.put("d", "4");
    map2.put("c", "3");
    map2.put("b", "2");
    map2.put("a", "1");

    assertEquals(map1, map2);
    assertEquals(map1.hashCode(), map2.hashCode());

    assertEquals(0, map1.indexOfKey("a"));
    assertEquals(3, map2.indexOfKey("a"));
  }
}

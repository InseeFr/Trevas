package fr.insee.vtl.engine.functions.providers;

import com.github.hervian.reflection.Fun;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public final class StringFunctionsProvider {

  private static final Pattern LTRIM = Pattern.compile("^\\s+");
  private static final Pattern RTRIM = Pattern.compile("\\s+$");

  public static String trim(String value) {
    if (value == null) {
      return null;
    }
    return value.trim();
  }

  public static String ltrim(String value) {
    if (value == null) {
      return null;
    }
    return LTRIM.matcher(value).replaceAll("");
  }

  public static String rtrim(String value) {
    if (value == null) {
      return null;
    }
    return RTRIM.matcher(value).replaceAll("");
  }

  public static String ucase(String value) {
    if (value == null) {
      return null;
    }
    return value.toUpperCase();
  }

  public static String lcase(String value) {
    if (value == null) {
      return null;
    }
    return value.toLowerCase();
  }

  public static Long len(String value) {
    if (value == null) {
      return null;
    }
    return (long) value.length();
  }

  public static String substr(String value, Long start, Long len) {
    if (value == null) {
      return null;
    }
    if (start == null) {
      start = 1L;
    }
    if (len == null) {
      len = Long.valueOf(value.length());
    }
    if (start > value.length()) {
      return "";
    }
    if (start != 0) {
      start = start - 1;
    }

    var end = start + len;
    if (end > value.length()) {
      return value.substring(Math.toIntExact(start));
    }
    return value.substring(Math.toIntExact(start), Math.toIntExact(end));
  }

  public static String replace(String value, String pattern, String replacement) {
    if (value == null || pattern == null) {
      return null;
    }
    if (replacement == null) {
      replacement = "";
    }
    return value.replaceAll(pattern, replacement);
  }

  public static Long instr(String v, String v2, Long start, Long occurence) {
    if (v == null || v2 == null) {
      return null;
    }
    if (start == null) {
      start = 0L;
    }
    if (occurence == null) {
      occurence = 1L;
    }
    return StringUtils.ordinalIndexOf(v.substring(start.intValue()), v2, occurence.intValue()) + 1L;
  }

  public Map<String, List<Method>> getFunctions() {
    Map<String, List<Method>> functions = new java.util.LinkedHashMap<>();
    functions.put("trim", List.of(Fun.toMethod(StringFunctionsProvider::trim)));
    functions.put("ltrim", List.of(Fun.toMethod(StringFunctionsProvider::ltrim)));
    functions.put("rtrim", List.of(Fun.toMethod(StringFunctionsProvider::rtrim)));
    functions.put("ucase", List.of(Fun.toMethod(StringFunctionsProvider::ucase)));
    functions.put("lcase", List.of(Fun.toMethod(StringFunctionsProvider::lcase)));
    functions.put("len", List.of(Fun.toMethod(StringFunctionsProvider::len)));
    functions.put("substr", List.of(Fun.toMethod(StringFunctionsProvider::substr)));
    functions.put("replace", List.of(Fun.toMethod(StringFunctionsProvider::replace)));
    functions.put("instr", List.of(Fun.toMethod(StringFunctionsProvider::instr)));
    return functions;
  }
}

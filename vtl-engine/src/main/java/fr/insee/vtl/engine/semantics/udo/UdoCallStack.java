package fr.insee.vtl.engine.semantics.udo;

import java.util.ArrayDeque;
import java.util.Deque;

/** Tracks active UDO names on the current thread to reject direct / mutual recursion. */
public final class UdoCallStack {

  private static final ThreadLocal<Deque<String>> ACTIVE = ThreadLocal.withInitial(ArrayDeque::new);

  private UdoCallStack() {}

  public static void enter(String operatorName) {
    Deque<String> stack = ACTIVE.get();
    if (stack.contains(operatorName)) {
      throw new IllegalStateException("recursive call to UDO '" + operatorName + "'");
    }
    stack.push(operatorName);
  }

  public static void leave(String operatorName) {
    Deque<String> stack = ACTIVE.get();
    if (!stack.isEmpty() && operatorName.equals(stack.peek())) {
      stack.pop();
    }
    if (stack.isEmpty()) {
      ACTIVE.remove();
    }
  }
}

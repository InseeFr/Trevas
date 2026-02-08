package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.util.Optional;

public class AlreadyDefinedException extends VtlScriptException {

  private static String format(Positioned identifier, Optional<Positioned> container) {
    var msg = "'%s', is already defined".formatted(identifier.getPosition().text());

    msg += container.map(c -> " in '%s'".formatted(c.getPosition().text())).orElse("");

    return msg;
  }

  private AlreadyDefinedException(Positioned identifier, Optional<Positioned> container) {
    super(format(identifier, container), identifier);
  }

  public AlreadyDefinedException(Positioned identifier) {
    super(format(identifier, Optional.empty()), identifier);
  }

  public AlreadyDefinedException(Positioned identifier, Positioned container) {
    super(format(identifier, Optional.of(container)), identifier);
  }
}

package fr.insee.vtl.model;

import java.time.Instant;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

import static java.lang.String.*;

// TODO: The spec specifies that date format should be configurable.
public abstract class VTLDate extends VTLObject<Instant> implements VTLTyped<VTLDate> {

    private VTLDate() {
        // private
    }

    @Override
    public Class<VTLDate> getVTLType() {
        return VTLDate.class;
    }

    public static VTLDate of(String input, String dateFormat, TimeZone timeZone) {

        if (!canParse(dateFormat)) {
            throw new RuntimeException(
                    format("Date format %s unsupported", dateFormat));
        }

        return new VTLDate() {

            @Override
            public Instant get() {
                DateTimeFormatter formatter =
                        DateTimeFormatter.ofPattern("yyyy");
                Year year = Year.parse(input, formatter);
                return year.atDay(1).atStartOfDay(timeZone.toZoneId()).toInstant();
            }
        };

    }

    public static VTLDate of(Instant instant) {

        return new VTLDate() {

            @Override
            public Instant get() {
                return instant;
            }
        };

    }

    public static boolean canParse(String dateFormat) {
        return "YYYY".equals(dateFormat);
    }

}

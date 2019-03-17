package kt.cli;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Date helper methods.
 */
public class Dates {
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    static String localDateTime(Instant instant) {
        return localDateTime(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));
    }

    static String localDateTime(LocalDateTime localDateTime) {
        return localDateTime.format(dateTimeFormatter);
    }
}

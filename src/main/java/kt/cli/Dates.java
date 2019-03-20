package kt.cli;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Date helper methods.
 */
@SuppressWarnings("WeakerAccess")
public class Dates {
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static String localDateTime(Instant instant) {
        return localDateTime(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));
    }

    public static String localDateTime(LocalDateTime localDateTime) {
        return localDateTime.format(dateTimeFormatter);
    }
}

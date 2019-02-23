package kt.cli;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper functionality for consumer start/end limits.
 */
public class ConsumerLimits {
    private static Pattern amountPattern = Pattern.compile("(\\d{1,18})([dDhHmMsS])");
    private static Pattern pastAmountPattern = Pattern.compile("(\\d{1,18})([dDhHmMsS])a");

    public static class ConsumerLimitCalculationException extends RuntimeException {
        public ConsumerLimitCalculationException(String message) {
            super(message);
        }
    }

    /**
     * Parse time amount.
     *
     * @param amountStr amount string; format: 0d, 0h, 0m,  0s
     * @return time amount
     * @throws ConsumerLimitCalculationException in case of parsing or validation errors
     */
    public static Duration amountOfTime(String amountStr) {
        Matcher matcher = amountPattern.matcher(amountStr);
        if (matcher.matches()) {
            long duration = Long.parseLong(matcher.group(1));
            String unit = matcher.group(2);
            switch (unit.toLowerCase()) {
                case "d":
                    return Duration.of(duration, ChronoUnit.DAYS);
                case "h":
                    return Duration.of(duration, ChronoUnit.HOURS);
                case "m":
                    return Duration.of(duration, ChronoUnit.MINUTES);
                case "s":
                    return Duration.of(duration, ChronoUnit.SECONDS);
                default:
                    throw new IllegalStateException("Unexpected amount unit " + unit);
            }
        }
        throw new ConsumerLimitCalculationException(String.format(
                "Could not parse amount '%s'. Supported formats - #d, #h, #m, #s.", amountStr));
    }

    /**
     * Calculate end consumer limit based on start consumer limit and consume duration.
     */
    public static LocalDateTime consumerEndLimit(LocalDateTime startConsumerLimit, Duration consumeDuration) {
        return startConsumerLimit.plus(consumeDuration);
    }

    /**
     * Calculates end consumer limit based on absolute date or relative duration
     * (ie. 6m represents 6 minutes ago).
     *
     * @param endConsumerLimit either an absolute date/time - {@link LocalTime}, {@link LocalDateTime}
     *                      or a relative duration conforming to - {@link ConsumerLimits#amountOfTime(String)}
     * @return an absolute local date representing the moment in the past the consumer should stop reading records.
     * @throws ConsumerLimitCalculationException in case of parsing or validation errors
     */
    public static LocalDateTime consumerEndLimit(String endConsumerLimit) {
        return calculateConsumerDateLimit(endConsumerLimit);
    }

    /**
     * Calculates start consumer limit based on absolute date or relative duration
     * (ie. 6m represents 6 minutes ago).
     *
     * @param startConsumerLimit either an absolute date/time - {@link LocalTime}, {@link LocalDateTime}
     *                      or a relative duration conforming to - {@link ConsumerLimits#amountOfTime(String)}
     * @return an absolute local date representing the moment in the past the consumer should start.
     * @throws ConsumerLimitCalculationException in case of parsing or validation errors
     */
    public static LocalDateTime consumerStartLimit(String startConsumerLimit) {
        return calculateConsumerDateLimit(startConsumerLimit);
    }

    private static LocalDateTime calculateConsumerDateLimit(String limitDateStr) {
        LocalDateTime limit = null;
        try {
            limit = LocalTime.parse(limitDateStr).atDate(LocalDate.now());
        } catch (DateTimeParseException e) {
            // ignore
        }

        try {
            limit = LocalDateTime.parse(limitDateStr);
        } catch (DateTimeParseException e) {
            // ignore
        }

        try {
            Duration duration = amountOfTime(limitDateStr);
            limit = LocalDateTime.now().minus(duration);
        } catch (ConsumerLimitCalculationException e) {
            // ignore
        }

        if (limit == null) {
            throw new ConsumerLimitCalculationException(String.format(
                    "Could not parse date '%s'. Supported formats - YYYY-MM-DDThh:mm:[ss], hh:mm:[ss], #d, #h, #m, #s.", limitDateStr));
        }

        if (limit.isAfter(LocalDateTime.now())) {
            throw new ConsumerLimitCalculationException(String.format(
                    "Consumer limit %s must be in the past.", limitDateStr));
        }

        return limit;
    }

}

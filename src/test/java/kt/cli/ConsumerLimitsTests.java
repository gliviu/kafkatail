package kt.cli;

import kt.cli.ConsumerLimits.ConsumerLimitCalculationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static kt.cli.ConsumerLimits.amountOfTime;
import static org.assertj.core.api.Assertions.*;

@DisplayName("Consumer Limits")
public class ConsumerLimitsTests {

    public static final Duration ERR = Duration.ofSeconds(1);

    @Nested
    @DisplayName("Amount")
    class ConsumeAmount {
        @DisplayName("calculates amount in seconds")
        @Test
        void t5755() {
            assertThat(amountOfTime("10s")).isEqualByComparingTo(Duration.ofSeconds(10));
            assertThat(amountOfTime("10S")).isEqualByComparingTo(Duration.ofSeconds(10));
        }

        @DisplayName("calculates amount in minutes")
        @Test
        void t2875() {
            assertThat(amountOfTime("10m")).isEqualByComparingTo(Duration.ofMinutes(10));
            assertThat(amountOfTime("10M")).isEqualByComparingTo(Duration.ofMinutes(10));
        }

        @DisplayName("calculates amount in hours")
        @Test
        void t69275() {
            assertThat(amountOfTime("10h")).isEqualByComparingTo(Duration.ofHours(10));
            assertThat(amountOfTime("10H")).isEqualByComparingTo(Duration.ofHours(10));
        }

        @DisplayName("calculates amount in days")
        @Test
        void t7514() {
            assertThat(amountOfTime("10d")).isEqualByComparingTo(Duration.ofDays(10));
            assertThat(amountOfTime("10D")).isEqualByComparingTo(Duration.ofDays(10));
        }

        @DisplayName("does not allow negative amount")
        @Test
        void t6582() {
            assertThatThrownBy(() -> amountOfTime("-10s")).isInstanceOf(ConsumerLimitCalculationException.class);
        }

        @DisplayName("requires amount unit")
        @Test
        void t3658() {
            assertThatThrownBy(() -> amountOfTime("10")).isInstanceOf(ConsumerLimitCalculationException.class);
        }

        @DisplayName("reports invalid amount unit")
        @Test
        void t3587() {
            assertThatThrownBy(() -> amountOfTime("10x")).isInstanceOf(ConsumerLimitCalculationException.class);
        }
    }

    @DisplayName("Start Limit")
    @Nested
    class ConsumerStartLimit {
        @DisplayName("calculates start date by absolute local date and time")
        @Test
        void t58311() {
            assertThat(ConsumerLimits.consumerStartLimit("2000-01-01T10:00:00"))
                    .isEqualTo(LocalDateTime.parse("2000-01-01T10:00:00"));
        }

        @DisplayName("calculates start date by absolute local time")
        @Test
        void t9658() {
            LocalDateTime dateTime = LocalDateTime.now()
                    .minus(1, ChronoUnit.SECONDS)
                    .truncatedTo(ChronoUnit.SECONDS);
            String localTime = dateTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            assertThat(ConsumerLimits.consumerStartLimit(localTime))
                    .isEqualTo(dateTime);
        }

        @DisplayName("calculates start date by relative amount")
        @Test
        void t2485() {
            LocalDateTime expected = LocalDateTime.now().minus(Duration.ofSeconds(200));
            assertThat(ConsumerLimits.consumerStartLimit("200s"))
                    .isBetween(expected.minus(ERR), expected.plus(ERR));
        }

        @DisplayName("does not allow start limit in the future")
        @Test
        void t5571() {
            assertThatThrownBy(() -> ConsumerLimits.consumerStartLimit(LocalDateTime.now().plusSeconds(1).toString()))
                    .isInstanceOf(ConsumerLimitCalculationException.class);
        }

        @DisplayName("reports invalid start limit")
        @Test
        void t21845() {
            assertThatThrownBy(() -> ConsumerLimits.consumerStartLimit("invalid limit"))
                    .isInstanceOf(ConsumerLimitCalculationException.class);
        }

    }

    @DisplayName("End Limit")
    @Nested
    class ConsumerEndLimit {
        @DisplayName("calculates end limit by start limit and duration")
        @Test
        void t6478() {
            assertThat(ConsumerLimits.consumerEndLimit(LocalDateTime.parse("2000-01-01T10:00:00"), Duration.ofMinutes(10)))
                    .isEqualTo(LocalDateTime.parse("2000-01-01T10:10:00"));
        }

        @DisplayName("calculates end limit by absolute local date")
        @Test
        void t3521() {
            LocalDateTime dateTime = LocalDateTime.now()
                    .minus(1, ChronoUnit.SECONDS)
                    .truncatedTo(ChronoUnit.SECONDS);
            String localTime = dateTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            assertThat(ConsumerLimits.consumerEndLimit(localTime))
                    .isEqualTo(dateTime);
        }

        @DisplayName("calculates end limit by absolute local time")
        @Test
        void t3487() {
            assertThat(ConsumerLimits.consumerEndLimit("2000-01-01T10:00:00"))
                    .isEqualTo(LocalDateTime.parse("2000-01-01T10:00:00"));
        }

        @DisplayName("calculates end limit by relative duration")
        @Test
        void t548() {
            LocalDateTime expected = LocalDateTime.now().minus(Duration.ofSeconds(200));
            assertThat(ConsumerLimits.consumerEndLimit("200s"))
                    .isBetween(expected.minus(ERR), expected.plus(ERR));
        }

        @DisplayName("does not allow end limit in the future")
        @Test
        void t5571() {
            assertThatThrownBy(() -> ConsumerLimits.consumerEndLimit(LocalDateTime.now().plusSeconds(1).toString()))
                    .isInstanceOf(ConsumerLimitCalculationException.class);
        }

        @DisplayName("reports invalid end limit")
        @Test
        void t9547() {
            assertThatThrownBy(() -> ConsumerLimits.consumerEndLimit("invalid limit"))
                    .isInstanceOf(ConsumerLimitCalculationException.class);
        }
    }


}

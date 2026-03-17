package io.github.veerakumarak.etl.utils;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

public class DateUtil {

    public static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");

    public static String getDate() {
        return LocalDate.now().toString();
    }

    public static LocalDate getCurDate() {
        return LocalDate.now();
    }

    public static LocalDateTime convertEpochMiliSecToLocalDateTime(long epochMiliSec, ZoneId zoneId) {
        Instant instant = Instant.ofEpochMilli(epochMiliSec);
        return LocalDateTime.ofInstant(instant, zoneId);
    }
    public static LocalDate convertToLocalDate(String julianDate) {

        julianDate = julianDate.trim();
        int fullYear;
        int dayOfYear;
        if(Objects.equals(julianDate, "999999")) {
            return LocalDate.parse("3000-12-31");
        }
        else if(julianDate.contains("-")){
            return LocalDate.parse(julianDate);
        } else if (julianDate.length() == 5) {
            fullYear = Integer.parseInt("19" + julianDate.substring(0, 2));
            dayOfYear = Integer.parseInt(julianDate.substring(2));
            if (dayOfYear == 0) {
                dayOfYear = 1;
            }
            return LocalDate.ofYearDay(fullYear, dayOfYear);
        } else {
            fullYear = (Character.getNumericValue(julianDate.charAt(0)) + 19) * 100 + Integer.parseInt(julianDate.substring(1, 3));
            dayOfYear = Integer.parseInt(julianDate.substring(3));
            if (dayOfYear == 0) {
                dayOfYear = 1;
            }
            return LocalDate.ofYearDay(fullYear, dayOfYear);
        }

    }

    public static String localDateToJulien(LocalDate gregorianDate) {
        if (gregorianDate == null) {
            return null;
        }

        int year = gregorianDate.getYear();
        int dayOfYear = gregorianDate.getDayOfYear();

        if (year == 2078) {
            return "999999";
        }

        int yearLastTwoDigits = year % 100;
        int numericJulianPart = (yearLastTwoDigits * 1000) + dayOfYear;

        String fiveDigitJulian = String.format("%05d", numericJulianPart);

        if ("79001".equals(fiveDigitJulian) && year != 1979) {
            fiveDigitJulian = "00000";
        }

        String centuryPrefix;
        if (year > 2000 && year < 2099) {
            centuryPrefix = "1";
        } else {
            centuryPrefix = "0";
        }

        return centuryPrefix + fiveDigitJulian;
    }


    public static LocalDate julianToGregorian(String julianDate) {
        // Input validation: Julian date cannot be null

        // Trim any leading/trailing whitespace from the input string
        String trimmedJulianDate = julianDate.trim();

        // Input validation: Julian date must be exactly 6 digits long
        if (trimmedJulianDate.length() != 6) {
            throw new IllegalArgumentException("Julian date must be 6 digits long. Received: \"" + trimmedJulianDate + "\" (length " + trimmedJulianDate.length() + ")");
        }

        try {
            if (trimmedJulianDate.equals("999999")) {
                return LocalDate.of(3000, 12, 31);
            }
            int yearTwoDigits = Integer.parseInt(trimmedJulianDate.substring(1, 3));
            int dayOfYear = Integer.parseInt(trimmedJulianDate.substring(3));
            int fullYear = 2000 + yearTwoDigits;
            return LocalDate.ofYearDay(fullYear, dayOfYear);

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Julian date format: Year or Day-of-Year part is not a valid number. " + e.getMessage(), e);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("Invalid Julian date: Day-of-Year is out of range for the calculated year. " + e.getMessage(), e);
        }
    }

    // Function to convert time from minutes/seconds to hours
    public static LocalTime convertMinutesSecondsToHours(String rprtTime) {
        if (rprtTime == null || rprtTime.isEmpty()) return null;

        Long convTimeToHours;
        int hour;
        int minute;
        int second;

        // If length of time is greater than 4 (assumed to be in minutes or seconds format)
        if (rprtTime.length() > 4) {
            convTimeToHours = Long.valueOf(rprtTime);
        } else {
            convTimeToHours = Long.parseLong(rprtTime) * 60;
        }

        if(convTimeToHours >= 24*60*60) {
            convTimeToHours -= 24*60*60;
        }

        // Convert the time
        return LocalTime.ofSecondOfDay(convTimeToHours);
    }
    public static String getFormattedDate(LocalDate date){
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        return date.format(dateFormatter);
    }

    public static String getFormattedTimestamp(LocalDateTime timestamp){
        DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        return timestamp.format(timestampFormatter);
    }

    public static String gregorianToJulien(LocalDate gregorianDate) {
        if (gregorianDate == null) {
            throw new IllegalArgumentException("Input Gregorian date cannot be null.");
        }

        int fullYear = gregorianDate.getYear();
        int twoDigitYear = fullYear % 100;
        int dayOfYear = gregorianDate.getDayOfYear();

        int combinedValue = twoDigitYear * 1000 + dayOfYear;
        String julianDateString = String.format("%05d", combinedValue);

        if (julianDateString.equals("79001") && fullYear != 1979) {
            julianDateString = "00000";
        }

        String finalJulYr = julianDateString.substring(0, 2);
        String finalJulDay = julianDateString.substring(2, 5);

        return "1" + finalJulYr + finalJulDay;
    }

    /**
     * Converts a Julian date string to a Gregorian LocalDate
     *
     * @param julianDate A string representing the Julian date in 'YYDDD' or 'CYYDDD' format.
     * @return A LocalDate object representing the converted date.
     * @throws IllegalArgumentException if the input is null, empty, non-numeric, or has an invalid length.
     */
    public static LocalDate julianToGregorianReplica(String julianDate) {
        if (julianDate == null || julianDate.isEmpty()) {
            throw new IllegalArgumentException("Input Julian date cannot be null or empty.");
        }

        String trimmedDate = julianDate.trim();
        try {
            if (trimmedDate.length() == 6) {
                if (trimmedDate.equals("000000") || trimmedDate.equals("100000")) {
                    return LocalDate.of(2078, 12, 31);
                }

                char centuryDigit = trimmedDate.charAt(0);
                int twoDigitYear = Integer.parseInt(trimmedDate.substring(1, 3));
                int dayOfYear = Integer.parseInt(trimmedDate.substring(3));

                int fullYear;
                if (centuryDigit == '1') {
                    fullYear = 2000 + twoDigitYear;
                } else {
                    fullYear = 1900 + twoDigitYear;
                }
                return LocalDate.of(fullYear, 1, 1).plusDays(dayOfYear - 1);
            }
            else if (trimmedDate.length() == 5) {
                if (trimmedDate.equals("00000")) {
                    return LocalDate.of(2078, 12, 31);
                }

                int julianAsInt = Integer.parseInt(trimmedDate);
                int twoDigitYear = Integer.parseInt(trimmedDate.substring(0, 2));
                int dayOfYear = Integer.parseInt(trimmedDate.substring(2));
                int fullYear;

                if (julianAsInt > 35000 && julianAsInt < 49366) {
                    fullYear = 1900 + twoDigitYear;
                } else {
                    if (twoDigitYear >= 50) {
                        fullYear = 1900 + twoDigitYear;
                    } else {
                        fullYear = 2000 + twoDigitYear;
                    }
                }
                return LocalDate.of(fullYear, 1, 1).plusDays(dayOfYear - 1);
            } else {
                throw new IllegalArgumentException("Julian date must be 5 or 6 digits long. Received length: " + trimmedDate.length());
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Julian date format: contains non-numeric characters.", e);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Resulting date is invalid.", e);
        }
    }

    public static Timestamp getTimestampFromLong(Long timestamp) {
        return new Timestamp(timestamp);
    }

    public static boolean isDateTimeInRange(LocalDateTime currentDate, LocalDateTime dEffStrt, LocalDateTime dEffEnd) {
        return !currentDate.isAfter(dEffEnd) && !currentDate.isBefore(dEffStrt);
    }

    /**
     * Check if a date falls within a date range (inclusive).
     * @param date The date to check
     * @param startDate Range start date
     * @param endDate Range end date
     * @return true if date is within range (inclusive)
     */
    public static boolean isDateInRange(LocalDate date, LocalDate startDate, LocalDate endDate) {
        if (date == null || startDate == null || endDate == null) {
            return false;
        }
        return !date.isBefore(startDate) && !date.isAfter(endDate);
    }

    /**
     * Check if a record is currently active based on date range and current flag.
     * Used for DDHUB records that have c_cur_in_dt_rng flag.
     * @param currentFlag The current-in-date-range flag (should be "C" for current)
     * @param date The date to check
     * @param startDate Range start date
     * @param endDate Range end date
     * @return true if record is current and date is within range
     */
    public static boolean isCurrentRecord(String currentFlag, LocalDate date, LocalDate startDate, LocalDate endDate) {
        return "C".equals(currentFlag) && isDateInRange(date, startDate, endDate);
    }
}

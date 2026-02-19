import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateUtils {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * startDate(yyyy-MM-dd)가 속한 분기의 마지막 날짜를 반환한다.
     */
    public static String getQuarterEndDate(String startDate) {
        LocalDate date = LocalDate.parse(startDate, FORMATTER);
        int month = date.getMonthValue();

        int quarterEndMonth;
        if (month <= 3) {
            quarterEndMonth = 3;
        } else if (month <= 6) {
            quarterEndMonth = 6;
        } else if (month <= 9) {
            quarterEndMonth = 9;
        } else {
            quarterEndMonth = 12;
        }

        LocalDate quarterEnd = LocalDate.of(date.getYear(), quarterEndMonth, 1)
                .withDayOfMonth(LocalDate.of(date.getYear(), quarterEndMonth, 1).lengthOfMonth());

        return quarterEnd.format(FORMATTER);
    }

    /**
     * startDate(yyyy-MM-dd)가 속한 연도의 연말 날짜(12-31)를 반환한다.
     */
    public static String getYearEndDate(String startDate) {
        LocalDate date = LocalDate.parse(startDate, FORMATTER);
        LocalDate yearEnd = LocalDate.of(date.getYear(), 12, 31);
        return yearEnd.format(FORMATTER);
    }
}

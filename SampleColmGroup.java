import java.util.Arrays;

public class SampleColmGroup implements ColmGroup {

    // 그룹화 기준으로 사용할 열의 인덱스 (0-based)
    private static final int[] GROUPING_COLUMNS = {0, 1};

    @Override
    public int[] getGroupingColms() {
        return GROUPING_COLUMNS;
    }

    /**
     * 두 레코드가 동일한 그룹에 속하는지 확인합니다.
     * GROUPING_COLUMNS에 정의된 모든 열의 값이 같아야 동일한 그룹으로 판단합니다.
     */
    @Override
    public boolean isSameGroup(String[] prev, String[] record) {
        for (int colIndex : GROUPING_COLUMNS) {
            // 하나라도 다르면 같은 그룹이 아님
            if (!prev[colIndex].equals(record[colIndex])) {
                return false;
            }
        }
        return true;
    }

    /**
     * 동일 그룹 내에서 레코드를 축적합니다.
     * prev 레코드가 누적의 기반이 됩니다.
     * @param prev 이전 레코드 (또는 현재 그룹의 누적 결과)
     * @param record 현재 레코드
     * @return 누적된 레코드
     */
    @Override
    public String[] accumulate(String[] prev, String[] record) {
        // prev가 null이면 현재 레코드를 반환 (그룹의 시작)
        if (prev == null) {
            return Arrays.copyOf(record, record.length);
        }

        // --- 데이터 축적 로직 ---
        // 예: 세 번째 열(인덱스 2)은 숫자 합산
        try {
            int prevValue = Integer.parseInt(prev[2]);
            int currentValue = Integer.parseInt(record[2]);
            prev[2] = String.valueOf(prevValue + currentValue);
        } catch (NumberFormatException e) {
            // 숫자가 아닌 경우, 이전 값 유지 또는 다른 처리
        }

        // 예: 네 번째 열(인덱스 3)은 문자열 연결
        prev[3] = prev[3] + "," + record[3];

        return prev;
    }

    /**
     * 그룹 처리가 끝날 때 호출됩니다.
     * 여기서는 특별한 로직이 없습니다.
     */
    @Override
    public void scale(String[] prev) {
        // 기본적으로 아무 작업도 하지 않음
        ColmGroup.super.scale(prev);
    }
}

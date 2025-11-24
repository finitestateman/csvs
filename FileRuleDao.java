import java.util.Map;

public interface FileRuleDao {
    Map<String, IsuFormula> getIsuFormula(String tableNm, String isuDt, String division);
}

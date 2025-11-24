import org.apache.ibatis.annotations.Param;
import java.util.List;
import java.util.Map;

public interface ExtrIntgrDao {
    void deleteExtrIntgr(Map<String, String> delParams);
    void insertExtrIntgrsBatch(
        @Param("record") String[] record,
        @Param("isuFileId") String isuFileId,
        @Param("tableNm") String tableNm,
        @Param("isuDt") String isuDt,
        @Param("rowCount") int rowCount,
        @Param("ownerId") String ownerId,
        @Param("cols") List<String> cols
    );
}

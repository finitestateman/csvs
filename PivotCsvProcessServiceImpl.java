import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class PivotCsvProcessServiceImpl {

    private final FileRuleDao fileRuleDao;
    private final SqlSessionTemplate batchSqlSession;
    private final String agentRunType;
    private final List<ResearchFileFormula> researchFileFormulas;

    private final int CHUNK_SIZE = 800;
    private final int MAX_COL_SIZE = 50;
    private final String[] colms = new String[MAX_COL_SIZE];

    // init block
    {
        for (int i = 0; i < colms.length; i++) {
            colms[i] = "COLM_" + (i + 1) + "_VAL_CONT";
        }
    }

    // constructor
    public PivotCsvProcessServiceImpl(
        FileRuleDao fileRuleDao,
        @Qualifier("batchSqlSession") SqlSessionTemplate batchSqlSession,
        @Value("${agent.run}") String agentRunType,
        List<ResearchFileFormula> researchFileFormulas
    ) {
        this.fileRuleDao = fileRuleDao;
        this.batchSqlSession = batchSqlSession;
        this.agentRunType = agentRunType;
        this.researchFileFormulas = researchFileFormulas;
    }

    public ResponseExtractor<TaskResult> parse(PivotCacheInfo pivotCacheInfo, String tableNm, ExtrLog extrLog) {

        final String isuFileId = extrLog.getStdFileNm();
        final String isuDt = extrLog.getRegYmd();
        final String division = extrLog.getGbmCd();

        final ResearchFileFormula researchFileFormula = this.selectStrategy(isuFileId, tableNm);
        final ColmGroup colmGroup = researchFileFormula instanceof ColmGroup ? (ColmGroup) researchFileFormula : null;

        ExtrIntgrDao mapper = batchSqlSession.getMapper(ExtrIntgrDao.class);
        final Map<String, IsuFormula> isuFormula = fileRuleDao.getIsuFormula(tableNm, isuDt, division);

        return resp -> {
            Map<String, String> delParams = new HashMap<>();
            delParams.put("tableNm", tableNm);
            delParams.put("regYmd", isuDt);
            mapper.deleteExtrIntgr(delParams);

            try (
                BufferedReader br = new BufferedReader(new InputStreamReader(resp.getBody(), StandardCharsets.UTF_8), 1 << 20);
                CSVParser csvParser = CSVFormat.DEFAULT.builder()
                .setHeader(pivotCacheInfo.getHeaders())
                .setQuote('"')
                .setTrim(true)
                .setNullString(null)
                .setSkipHeaderRecord(true)
                .setIgnoreSurroundingSpaces(true)
                .setIgnoreHeaderCase(false)
                .build()
                .parse(br)
            ) {

                List<IsuFormula> originalFormulas = new ArrayList<>(IsuFormula.values());
                List<IsuFormula> formulas = new ArrayList<>();
                if (researchFileFormula != null) {
                    formulas.addAll(researchFileFormula.removeUsedFormula(originalFormulas));
                } else {
                    formulas.addAll(originalFormulas);
                }

                // In-memory processing logic for interleaved groups
                if (colmGroup != null && colmGroup.isInMemoryProcessingRequired()) {
                    Map<String, String[]> groupedData = new LinkedHashMap<>();

                    for (CSVRecord csvRecord : csvParser) {
                        String[] record = parseCsvRecord(csvRecord, isuFormula);
                        if (isEveryFieldNull(record)) continue;

                        // Apply formula logic
                        if (researchFileFormula != null) {
                            researchFileFormula.applyFormula(record, originalFormulas, csvRecord, tableNm);
                        }
                        for (IsuFormula formula : formulas) {
                            String formulaExpr = formula.getFormulaExpr();
                            int i = formula.getColmOrder() - 1;
                            String field = record[i];
                            if (formulaExpr != null && field != null) {
                                record[i] = this.applyFormula(field, formulaExpr);
                            }
                        }

                        String groupKey = Arrays.stream(colmGroup.getGroupingColms())
                                               .mapToObj(i -> record[i])
                                               .collect(Collectors.joining("-"));

                        String[] prevRecord = groupedData.get(groupKey);
                        String[] accumulatedRecord = colmGroup.accumulate(prevRecord, record);
                        groupedData.put(groupKey, accumulatedRecord);
                    }

                    int rowCount = 0;
                    List<String> cols = Arrays.asList(colms);
                    for (String[] finalRecord : groupedData.values()) {
                        colmGroup.scale(finalRecord);
                        mapper.insertExtrIntgrsBatch(finalRecord, isuFileId, tableNm, isuDt, ++rowCount, MyConst.OWNER_ID, cols.subList(0, finalRecord.length));
                    }

                // Original streaming logic for sequential groups
                } else {
                    String[] prev = null;
                    int rowCount = 0;
                    List<String> cols = Arrays.asList(colms);

                    for (CSVRecord csvRecord : csvParser) {
                        String[] record = parseCsvRecord(csvRecord, isuFormula);
                        if (isEveryFieldNull(record)) continue;
                        
                        // Apply formula logic
                        if (researchFileFormula != null) {
                            researchFileFormula.applyFormula(record, originalFormulas, csvRecord, tableNm);
                        }
                        for (IsuFormula formula : formulas) {
                            String formulaExpr = formula.getFormulaExpr();
                            int i = formula.getColmOrder() - 1;
                            String field = record[i];
                            if (formulaExpr != null && field != null) {
                                record[i] = this.applyFormula(field, formulaExpr);
                            }
                        }

                        if (prev != null) {
                            if (colmGroup != null && !colmGroup.isSameGroup(prev, record)) {
                                colmGroup.scale(prev);
                                mapper.insertExtrIntgrsBatch(prev, isuFileId, tableNm, isuDt, rowCount, MyConst.OWNER_ID, cols.subList(0, prev.length));
                                prev = null;
                            }
                        }

                        if (colmGroup != null) {
                            prev = colmGroup.accumulate(prev, record);
                        } else {
                            prev = record;
                        }
                        
                        rowCount++;
                        if (rowCount % CHUNK_SIZE == 0) {
                            if(prev != null) {
                               // This part of logic might need adjustment depending on desired behavior for chunking with grouping
                            }
                            batchSqlSession.flushStatements();
                        }
                    }

                    if (prev != null) {
                        if (colmGroup != null) {
                            colmGroup.scale(prev);
                        }
                        mapper.insertExtrIntgrsBatch(prev, isuFileId, tableNm, isuDt, rowCount, MyConst.OWNER_ID, cols.subList(0, prev.length));
                    }
                }
                batchSqlSession.flushStatements();

            } catch (Exception e) {
                // logging
                throw e;
            }

            return new TaskResult(true);
        };
    }

    private String[] parseCsvRecord(CSVRecord csvRecord, Map<String, IsuFormula> isuFormula) {
        String[] record = new String[isuFormula.size()];
        for (Map.Entry<String, IsuFormula> entry : isuFormula.entrySet()) {
            String headerName = entry.getKey();
            int i = entry.getValue().getColmOrder() - 1;
            if (i < record.length) {
                try {
                    record[i] = csvRecord.get(headerName);
                } catch (IllegalArgumentException e) {
                    // pass
                }
            }
        }
        setNullForNAField(record);
        return record;
    }

    public String applyFormula(String val, String formula) {
        // some logic
        return val;
    }

    public void setNullForNAField(String[] record) {
        for (int i = 0; i < record.length; i++) {
            if ("#N/A".equalsIgnoreCase(record[i]) || "".equals(record[i])) {
                record[i] = null;
            }
        }
    }

    public boolean isEveryFieldNull(String[] record) {
        for (String s : record) {
            if (s != null) {
                return false;
            }
        }
        return true;
    }

    public ResearchFileFormula selectStrategy(String isuFileId, String tableNm) {
        return researchFileFormulas
            .stream()
            .filter(r ->
                Arrays.asList(r.isuFileIds).contains(isuFileId)
                && (Arrays.asList(r.tableNms).contains(tableNm) || r.tableNms.length == 0))
            .findFirst()
            .orElse(null);
    }
}

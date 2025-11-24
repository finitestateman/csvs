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
        this.agentRunType = agentRunType
        this.researchFileFormulas = researchFileFormulas
    }

    public ResponseExtractor<TaskResult> parse(PivotCacheInfo pivotCacheInfo, String tableNm, ExtrLog extrLog) {

        final String traceID = extrLog.getTraceID();
        final String isuFileId = extrLog.getStdFileNm();
        final String isuDt = extrLog.getRegYmd();
        final String division = extrLog.getGbmCd();

        final ResearchFileFormula researchFileFormula = this.selectStrategy(isuFileId, tableNm);
        final ColmGroup colmGroup = researchFileFormula instanceof ColmGroup ? (ColmGroup) researchFileFormula : null;

        List<String> cols = Arrays.asList(colms);

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

                String[] prev = null;

                int rowCount = 0;
                String[] record;
                for (CSVRecord csvRecord : csvParser) {
                    record = new String[isuFormula.size()];

                    // csvRecord.values() 중에서 isuFormula에 존재하는 것만 추리고 순서도 바꾼다
                    for (Entry<String, IsuFormula> entry : isuFormula.entrySet()) {
                        String headerName = entry.getKey();
                        // colmOrder는 1부터 시작
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

                    if (isEveryFieldNull(record)) {
                        continue;
                    }
                    
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
                        if (colmGroup != null) {
                            if (!colmGroup.isSameGroup(prev, record)) {
                                colmGroup.scale(prev);
                            }
                        }

                        mapper.insertExtrIntgrsBatch(prev, isuFileId, tableNm, isuDt, rowCount, MyConst.OWNER_ID, cols.subList(0, prev.length));
                        prev = null;
                    }

                    if (colmGroup != null) {
                        prev = colmGroup.accumulate(prev, record);
                    } else {
                        prev = record;
                    }

                    rowCount++;

                    if (rowCount % CHUNK_SIZE == 0) {
                        batchSqlSession.flushStatements();
                    }
                }

                // 나머지 처리
                if (prev != null) {
                    if (colmGroup != null) {
                        colmGroup.scale(prev);
                    }

                    mapper.insertExtrIntgrsBatch(prev, isuFileId, tableNm, isuDt, rowCount, MyConst.OWNER_ID, cols.subList(0, prev.length));
                }
                batchSqlSession.flushStatements();
            } catch (IllegalArgumentException e) {
                // logging                
                throw e;
            } catch (Exception e) {
                // logging
                throw e;
            }

            return new TaskResult(true);
        }
    }

    public String applyFormula(String val, String formula) {
        // some logic
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
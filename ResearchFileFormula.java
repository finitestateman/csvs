import org.apache.commons.csv.CSVRecord;

import java.util.List;

public abstract class ResearchFileFormula {
    public String[] isuFileIds;
    public String[] tableNms;

    public abstract List<IsuFormula> removeUsedFormula(List<IsuFormula> originalFormulas);
    public abstract void applyFormula(String[] record, List<IsuFormula> originalFormulas, CSVRecord csvRecord, String tableNm);
}

public enum IsuFormula {
    // This is a placeholder.
    // Add actual enum constants here.
    FORMULA_1(1, "some_expression"),
    FORMULA_2(2, "another_expression");

    private final int colmOrder;
    private final String formulaExpr;

    IsuFormula(int colmOrder, String formulaExpr) {
        this.colmOrder = colmOrder;
        this.formulaExpr = formulaExpr;
    }

    public int getColmOrder() {
        return colmOrder;
    }

    public String getFormulaExpr() {
        return formulaExpr;
    }
}

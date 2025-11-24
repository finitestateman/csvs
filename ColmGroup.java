public interface ColmGroup {
    int[] getGroupingColms();

    default boolean isInMemoryProcessingRequired() {
        return false;
    }

    default boolean isSameGroup(String[] prev, String[] record) {
        for (int i = 0; i < getGroupingColms().length; i++) {
		    if (!prev[i].equals(record[i])) {
			    return false;
            }
        }
        return true;
    }

    String[] accumulate(String[] prev, String[] record);

    default void scale(String[] prev) {
	    // does nothing by default
    }

    default int[] rangeList(int count) {
        return java.util.stream.IntStream.range(0, count).toArray();
    }

}
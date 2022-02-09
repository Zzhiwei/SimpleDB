package simpledb.parse;

public class Pair {
	private final String field;
	private final boolean isAscending;
	
	public Pair(String field, boolean isAscending) {
		this.field = field;
		this.isAscending = isAscending;
	}
	
	public String getField() {
		return field;
	}
	
	public boolean isAscending() {
		return isAscending;
	}
}

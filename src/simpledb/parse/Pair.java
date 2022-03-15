package simpledb.parse;

/**
 * Pair class representing a pair of field name String and isAscending boolean data.
 */
public class Pair {
	private final String field;
	private final boolean isAscending;
	
	/**
	 * Creates a field name and isAscending boolean pair.
	 * @param field Field name
	 * @param isAscending True if query orders field in ascending order, else false
	 */
	public Pair(String field, boolean isAscending) {
		this.field = field;
		this.isAscending = isAscending;
	}
	
	/**
	 * Returns field name.
	 * @return field name
	 */
	public String getField() {
		return field;
	}
	
	/**
	 * Returns true if field is to be in ascending order, else false. 
	 * @return true if field is to be in ascending order, else false
	 */
	public boolean isAscending() {
		return isAscending;
	}
}

package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

	private static final long serialVersionUID = 1L;
	private PageId pageID;
	private int tupleNum;

	/**
	 * Creates a new RecordId referring to the specified PageId and tuple number.
	 * 
	 * @param pid     the pageid of the page on which the tuple resides
	 * @param tupleno the tuple number within the page.
	 */
	public RecordId(PageId pid, int tupleno) {
		// some code goes here
		pageID = pid;
		tupleNum = tupleno;
	}

	/**
	 * @return the tuple number this RecordId references.
	 */
	public int getTupleNumber() {
		// some code goes here
		return tupleNum;
	}

	public void setPageId(PageId pid) throws DbException {
		if (pid == null)
			throw new DbException("pid can't be null");
		pageID = pid;
	}

	public void setTupleNumber(int tupleNum) throws DbException {
		if (tupleNum < 0)
			throw new DbException("tuple number can't be negative");
		this.tupleNum = tupleNum;
	}

	/**
	 * @return the page id this RecordId references.
	 */
	public PageId getPageId() {
		// some code goes here
		return pageID;
	}

	/**
	 * Two RecordId objects are considered equal if they represent the same tuple.
	 * 
	 * @return True if this and o represent the same tuple
	 */
	@Override
	public boolean equals(Object o) {
		// some code goes here
		// throw new UnsupportedOperationException("implement this");
//        if(o == null || o.getClass() != RecordId.class) return false;
//        return ((RecordId) o).hashCode() == hashCode();
		if (o instanceof RecordId) {
			RecordId arg = (RecordId) o;
			if (getPageId().equals(arg.getPageId()) && tupleNum == arg.getTupleNumber()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * You should implement the hashCode() so that two equal RecordId instances
	 * (with respect to equals()) have the same hashCode().
	 * 
	 * @return An int that is the same for equal RecordId objects.
	 */
	@Override
	public int hashCode() {
		// some code goes here
		// concatenate the hashcode of its PageID and the the tuple number
//        int pageHashCode = getPageId().hashCode();
//        return Integer.parseInt(Integer.toString(pageHashCode) + Integer.toString(getTupleNumber()));
		String hash = getPageId().hashCode() + "" + tupleNum;
		return hash.hashCode();
	}
}
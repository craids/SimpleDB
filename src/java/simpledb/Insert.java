package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

	private static final long serialVersionUID = 1L;

	private TransactionId tid;
	private OpIterator child;
	private int tableID;
	private TupleDesc resDesc;
	private boolean hasInserted;

	/**
	 * Constructor.
	 *
	 * @param t       The transaction running the insert.
	 * @param child   The child operator from which to read tuples to be inserted.
	 * @param tableId The table in which to insert tuples.
	 * @throws DbException if TupleDesc of child differs from table into which we
	 *                     are to insert.
	 */
	public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
		// some code goes here
		tid = t;
		this.child = child;
		tableID = tableId;
		Type[] types = new Type[1];
		String[] names = new String[1];
		types[0] = Type.INT_TYPE;
		resDesc = new TupleDesc(types, names);
		hasInserted = false;
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return resDesc;
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		child.open();
		super.open();
		hasInserted = false;
	}

	public void close() {
		// some code goes here
		child.close();
		super.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		close();
		open();
	}

	/**
	 * Inserts tuples read from child into the tableId specified by the constructor.
	 * It returns a one field tuple containing the number of inserted records.
	 * Inserts should be passed through BufferPool. An instances of BufferPool is
	 * available via Database.getBufferPool(). Note that insert DOES NOT need check
	 * to see if a particular tuple is a duplicate before inserting it.
	 *
	 * @return A 1-field tuple containing the number of inserted records, or null if
	 *         called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (hasInserted)
			return null;
		int cnt = 0;
		BufferPool bpool = Database.getBufferPool();
		while (child.hasNext()) {
			Tuple t = child.next();
			try {
				bpool.insertTuple(tid, tableID, t);
			} catch (IOException e) {
				e.printStackTrace();
				throw new DbException("Error inserting tuple via BufferPool");
			}
			cnt++;
		}

		Tuple ret = new Tuple(resDesc);
		Field dataField = new IntField(cnt);
		ret.setField(0, dataField);
		hasInserted = true;
		return ret;
	}

	@Override
	public OpIterator[] getChildren() {
		// some code goes here
		OpIterator[] ret = new OpIterator[1];
		ret[0] = child;
		return ret;
	}

	@Override
	public void setChildren(OpIterator[] children) {
		// some code goes here
		assert children.length == 1;
		child = children[0];
	}
}
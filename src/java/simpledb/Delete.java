package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

	private static final long serialVersionUID = 1L;

	private TransactionId tid;
	private OpIterator child;
	private TupleDesc resDesc;
	private boolean hasDeleted;

	/**
	 * Constructor specifying the transaction that this delete belongs to as well as
	 * the child to read from.
	 * 
	 * @param t     The transaction this delete runs in
	 * @param child The child operator from which to read tuples for deletion
	 */
	public Delete(TransactionId t, OpIterator child) {
		// some code goes here
		tid = t;
		this.child = child;
		Type[] types = new Type[1];
		String[] names = new String[1];
		types[0] = Type.INT_TYPE;
		resDesc = new TupleDesc(types, names);
		hasDeleted = false;
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return resDesc;
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		child.open();
		super.open();
		hasDeleted = false;
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
	 * Deletes tuples as they are read from the child operator. Deletes are
	 * processed via the buffer pool (which can be accessed via the
	 * Database.getBufferPool() method.
	 * 
	 * @return A 1-field tuple containing the number of deleted records.
	 * @see Database#getBufferPool
	 * @see BufferPool#deleteTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (hasDeleted)
			return null;
		int cnt = 0;
		BufferPool bpool = Database.getBufferPool();
		while (child.hasNext()) {
			Tuple t = child.next();
			try {
				bpool.deleteTuple(tid, t);
			} catch (IOException e) {
				e.printStackTrace();
				throw new DbException("Error inserting tuple via BufferPool");
			}
			cnt++;
		}

		Tuple ret = new Tuple(resDesc);
		Field dataField = new IntField(cnt);
		ret.setField(0, dataField);
		hasDeleted = true;
		return ret;
	}

	@Override
	public OpIterator[] getChildren() {
		// some code goes here
		OpIterator[] ret = new OpIterator[1];
		ret[0] = child;
		return null;
	}

	@Override
	public void setChildren(OpIterator[] children) {
		// some code goes here
		assert children.length == 1;
		child = children[0];
	}
}
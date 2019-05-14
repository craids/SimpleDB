package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

	private static final long serialVersionUID = 1L;

	private OpIterator childOperator;
	private Predicate pred;

	/**
	 * Constructor accepts a predicate to apply and a child operator to read tuples
	 * to filter from.
	 * 
	 * @param p     The predicate to filter tuples with
	 * @param child The child operator
	 */
	public Filter(Predicate p, OpIterator child) {
		// some code goes here
		pred = p;
		childOperator = child;
	}

	public Predicate getPredicate() {
		// some code goes here
		return pred;
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return childOperator.getTupleDesc();
	}

	public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
		// some code goes here
		childOperator.open();
		super.open();

	}

	public void close() {
		// some code goes here
		childOperator.close();
		super.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		close();
		open();
	}

	/**
	 * AbstractDbIterator.readNext implementation. Iterates over tuples from the
	 * child operator, applying the predicate to them and returning those that pass
	 * the predicate (i.e. for which the Predicate.filter() returns true.)
	 * 
	 * @return The next tuple that passes the filter, or null if there are no more
	 *         tuples
	 * @see Predicate#filter
	 */
	protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
		// some code goes here
		Tuple ret;
		while (childOperator.hasNext()) {
			ret = childOperator.next();
			if (pred.filter(ret))
				return ret;
		}
		return null;
	}

	@Override
	public OpIterator[] getChildren() {
		// some code goes here
		OpIterator[] childOperators = new OpIterator[1];
		childOperators[0] = childOperator;
		return childOperators;
	}

	@Override
	public void setChildren(OpIterator[] children) {
		// some code goes here
		assert children.length == 1;
		childOperator = children[0];
	}
}
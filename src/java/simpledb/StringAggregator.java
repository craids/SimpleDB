package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Map<Object, Tuple> groups;
	private TupleDesc resDesc;
	private Tuple noGbRes;
	private Aggregator.Op op;
	private boolean isDescSet;

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or
	 *                    NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
	 *                    null if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		if (what != Op.COUNT)
			throw new IllegalArgumentException();
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		op = what;
		isDescSet = false;
	}

	private Tuple getZeroCountTuple(Tuple tup) {
		Tuple t = new Tuple(resDesc);
		Field zeroCounterField = new IntField(0);
		if (gbfield == NO_GROUPING) {
			t.setField(0, zeroCounterField);
		} else {
			t.setField(1, zeroCounterField);
			t.setField(0, tup.getField(gbfield));
		}
		return t;
	}

	private void incrementTupleCount(Tuple tup) {
		int fieldIdx = (gbfield == NO_GROUPING ? 0 : 1);
		int currCnt = ((IntField) tup.getField(fieldIdx)).getValue();
		currCnt++;
		Field updatedCounterField = new IntField(currCnt);
		tup.setField(fieldIdx, updatedCounterField);
	}

	private void setResultTupleSchema(Tuple tup) {
		Type[] tdTypes;
		String[] tdNames;
		String afieldName = op.toString() + "(" + tup.getField(afield).toString() + ")";

		if (gbfield == NO_GROUPING) {
			assert gbfieldtype == null;
			tdTypes = new Type[1];
			tdNames = new String[1];
			tdTypes[0] = Type.INT_TYPE;
			tdNames[0] = afieldName;
			resDesc = new TupleDesc(tdTypes, tdNames);
			noGbRes = getZeroCountTuple(tup);

		} else {
			tdTypes = new Type[2];
			tdNames = new String[2];
			tdTypes[0] = gbfieldtype;
			tdTypes[1] = Type.INT_TYPE;
			tdNames[0] = tup.getTupleDesc().getFieldName(gbfield);
			tdNames[1] = afieldName;

			resDesc = new TupleDesc(tdTypes, tdNames);
			groups = new HashMap<Object, Tuple>();
		}
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here
		if (!isDescSet) {
			setResultTupleSchema(tup);
			isDescSet = true;
		}

		if (gbfield == NO_GROUPING) {
			incrementTupleCount(noGbRes);
			return;
		}

		Field groupByField = tup.getField(gbfield);
		Object key;
		if (gbfieldtype == Type.INT_TYPE)
			key = ((IntField) groupByField).getValue();

		else // STRING_TYPE
			key = ((StringField) groupByField).getValue();

		if (groups.containsKey(key)) {
			incrementTupleCount(groups.get(key));

		} else {
			Tuple t = getZeroCountTuple(tup);
			incrementTupleCount(t);
			groups.put(key, t);
		}
	}

	/**
	 * Create a OpIterator over group aggregate results.
	 *
	 * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if
	 *         using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in the
	 *         constructor.
	 */
	public OpIterator iterator() {
		// some code goes here
		List<Tuple> tups = new LinkedList<>();
		if (gbfield == NO_GROUPING) {
			tups.add(noGbRes);
		} else {
			for (Object key : groups.keySet())
				tups.add(groups.get(key));
		}
		return new TupleIterator(resDesc, tups);
	}

}

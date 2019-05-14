package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

	private static final long serialVersionUID = 1L;

	private ArrayList<Type> types;
	private TupleDesc schema;
	private RecordId recordID;
	private ArrayList<Field> fields;

	/**
	 * Create a new tuple with the specified schema (type).
	 *
	 * @param td the schema of this tuple. It must be a valid TupleDesc instance
	 *           with at least one field.
	 */
	public Tuple(TupleDesc td) {
		if (td == null)
			return;
		int fieldsNum = td.numFields();
		if (fieldsNum < 1)
			return;
		schema = td;
		fields = new ArrayList<>(fieldsNum);
		types = new ArrayList<>(td.numFields());
		Iterator<TDItem> itd = td.iterator();
		int i = 0;
		while (itd.hasNext()) {
			types.add(itd.next().fieldType);
			fields.add(null);
			i++;
		}
		recordID = null;
	}

	/**
	 * @return The TupleDesc representing the schema of this tuple.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return schema;
	}

	/**
	 * @return The RecordId representing the location of this tuple on disk. May be
	 *         null.
	 */
	public RecordId getRecordId() {
		// some code goes here
		return recordID;
	}

	/**
	 * Set the RecordId information for this tuple.
	 *
	 * @param rid the new RecordId for this tuple.
	 */
	public void setRecordId(RecordId rid) {
		// some code goes here
		recordID = rid;
	}

	/**
	 * Change the value of the ith field of this tuple.
	 *
	 * @param i index of the field to change. It must be a valid index.
	 * @param f new value for the field.
	 */
	public void setField(int i, Field f) {
		// some code goes here
		if (i < 0 || i >= fields.size()) {
			throw new NoSuchElementException();
		}
		if (f instanceof IntField && types.get(i) == Type.INT_TYPE) {
			fields.set(i, (IntField) f);
			return;
		}
		if (f instanceof StringField && types.get(i) == Type.STRING_TYPE) {
			fields.set(i, (StringField) f);
			return;
		}
		throw new NoSuchElementException();
	}

	/**
	 * @return the value of the ith field, or null if it has not been set.
	 *
	 * @param i field index to return. Must be a valid index.
	 */
	public Field getField(int i) {
		// some code goes here
		if (i >= fields.size() || i < 0)
			return null;
		return fields.get(i);
	}

	/**
	 * Returns the contents of this Tuple as a string. Note that to pass the system
	 * tests, the format needs to be as follows:
	 *
	 * column1\tcolumn2\tcolumn3\t...\tcolumnN
	 *
	 * where \t is any whitespace (except a newline)
	 */
	public String toString() {
		// some code goes here
		String ret = "";
		for (Field f : fields) {
			if (ret.length() > 0) {
				ret += "\t";
			}
			ret += f.toString();
		}
		return ret;
	}

	/**
	 * @return An iterator which iterates over all the fields of this tuple
	 */
	public Iterator<Field> fields() {
		// some code goes here
		return fields.iterator();
	}

	/**
	 * reset the TupleDesc of this tuple (only affecting the TupleDesc)
	 */
	public void resetTupleDesc(TupleDesc td) {
		// some code goes here
		schema = td;
	}
}

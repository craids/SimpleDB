package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private List<TDItem> items;

	/**
	 * A help class to facilitate organizing the information of each field
	 */
	public static class TDItem implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * The type of the field
		 */
		public final Type fieldType;

		/**
		 * The name of the field
		 */
		public final String fieldName;

		public TDItem(Type t, String n) {
			this.fieldName = n;
			this.fieldType = t;
		}

		public String toString() {
			return fieldName + "(" + fieldType + ")";
		}
	}

	/**
	 * @return An iterator which iterates over all the field TDItems that are
	 *         included in this TupleDesc
	 */
	public Iterator<TDItem> iterator() {
		// some code goes here
		return items.iterator();
	}

	private static final long serialVersionUID = 1L;

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the specified
	 * types, with associated named fields.
	 * 
	 * @param typeAr  array specifying the number of and types of fields in this
	 *                TupleDesc. It must contain at least one entry.
	 * @param fieldAr array specifying the names of the fields. Note that names may
	 *                be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		// some code goes here
		if (fieldAr != null) {
			assert typeAr.length == fieldAr.length;
		}

		this.items = new ArrayList<>();
		for (int i = 0; i < typeAr.length; i++) {
			TDItem item = new TDItem(typeAr[i], (fieldAr == null ? null : fieldAr[i]));
			this.items.add(item);
		}
	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with fields of
	 * the specified types, with anonymous (unnamed) fields.
	 * 
	 * @param typeAr array specifying the number of and types of fields in this
	 *               TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr) {
		// some code goes here
		this(typeAr, null);
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		// some code goes here
		return items.size();
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 * 
	 * @param i index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		// some code goes here
		if (i >= items.size())
			throw new NoSuchElementException();
		return items.get(i).fieldName;
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 * 
	 * @param i The index of the field to get the type of. It must be a valid index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public Type getFieldType(int i) throws NoSuchElementException {
		// some code goes here
		if (i >= items.size())
			throw new NoSuchElementException();
		return items.get(i).fieldType;
	}

	/**
	 * Find the index of the field with a given name.
	 * 
	 * @param name name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException if no field with a matching name is found.
	 */
	public int fieldNameToIndex(String name) throws NoSuchElementException {
		// some code goes here
		for (int i = 0; i < items.size(); i++) {
			String item = items.get(i).fieldName;
			if (item == null)
				continue;
			if (item.equals(name))
				return i;
		}
		throw new NoSuchElementException();
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note
	 *         that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		// some code goes here
		int totalSize = 0;
		for (TDItem item : items) {
			totalSize += item.fieldType.getLen();
		}
		return totalSize;
	}

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 * 
	 * @param td1 The TupleDesc with the first fields of the new TupleDesc
	 * @param td2 The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
		// some code goes here
		String[] fieldNames = new String[td1.numFields() + td2.numFields()];
		Type[] fieldTypes = new Type[fieldNames.length];
		int i = 0;
		Iterator ite = td1.iterator();
		while (ite.hasNext()) {
			TDItem item = (TDItem) ite.next();
			fieldNames[i] = item.fieldName;
			fieldTypes[i] = item.fieldType;
			i++;
		}
		ite = td2.iterator();
		while (ite.hasNext()) {
			TDItem item = (TDItem) ite.next();
			fieldNames[i] = item.fieldName;
			fieldTypes[i] = item.fieldType;
			i++;
		}
		TupleDesc mergedDesc = new TupleDesc(fieldTypes, fieldNames);
		return mergedDesc;
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they have the same number of items and if
	 * the i-th type in this TupleDesc is equal to the i-th type in o for every i.
	 * 
	 * @param o the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */

	public boolean equals(Object o) {
		// some code goes here
		if (o == null)
			return false;
		if (o.getClass() != TupleDesc.class)
			return false;

		TupleDesc anotherDesc = (TupleDesc) o;
		if (anotherDesc.getSize() != getSize())
			return false;
		if (anotherDesc.numFields() != numFields())
			return false;
		for (int i = 0; i < numFields(); i++) {
			String thisItemName = items.get(i).fieldName;
			String anotherItemName = anotherDesc.getFieldName(i);
			if (thisItemName == null)
				return anotherItemName == null;
			if (anotherItemName == null)
				return false;

			if (!thisItemName.equals(anotherItemName))
				return false;
			Type thisItemType = items.get(i).fieldType;
			Type anotherItemType = anotherDesc.getFieldType(i);
			if (!thisItemType.equals(anotherItemType))
				return false;
		}
		return true;
	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		return items.hashCode();
//        throw new UnsupportedOperationException("unimplemented");
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the
	 * exact format does not matter.
	 * 
	 * @return String describing this descriptor.
	 */
	public String toString() {
		// some code goes here
		String ret = "";
		for (TDItem item : items) {
			if (ret.length() > 0) {
				ret += ", ";
			}
			ret += (item.fieldType.toString() + "(" + (item.fieldName == null ? "" : item.fieldName) + ")");
		}
		return ret;
	}

	public TupleDesc makeCopyWithAlias(String alias) {
		// TODO
		int itemLen = items.size();
		Type[] typeAr = new Type[itemLen];
		String[] fieldAr = new String[itemLen];
		for (int i = 0; i < itemLen; i++) {
			TDItem item = items.get(i);
			String originalName = item.fieldName;
			if (originalName == null)
				originalName = "null";

			if (alias == null)
				alias = "null";

			fieldAr[i] = alias + "." + originalName;
			Type type = item.fieldType;
			typeAr[i] = type;
		}
		TupleDesc ret = new TupleDesc(typeAr, fieldAr);
		return ret;
	}
}
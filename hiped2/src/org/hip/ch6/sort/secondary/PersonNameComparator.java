package org.hip.ch6.sort.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PersonNameComparator extends WritableComparator{

	protected PersonNameComparator(){
		super(Person.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		Person p1 = (Person) o1;
		Person p2 = (Person) o2;
		
		return p1.getLastName().compareTo(p2.getLastName());
	}
}

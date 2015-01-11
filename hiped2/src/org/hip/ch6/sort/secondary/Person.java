package org.hip.ch6.sort.secondary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Person implements WritableComparable<Person>{

	private String firstName;
	private String lastName;
			
	public Person() {
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(lastName);
		out.writeUTF(firstName);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.lastName = in.readUTF();
		this.firstName = in.readUTF();
	}

	@Override
	public int compareTo(Person o) {
		int cmp = this.lastName.compareTo(o.getLastName());
		if (cmp != 0)
			return cmp;
		
		return this.firstName.compareTo(o.getFirstName());
	}

	public void set(String lastName, String firstName){
		this.lastName = lastName;
		this.firstName = firstName;
	}
}

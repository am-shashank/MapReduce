/**
 * 
 */
package edu.upenn.cis455.mapreduce.worker;

import java.util.ArrayList;

/** Class which holds a key and a list of values associated with the key
 * @author Shashank
 *
 */
public class KeyValues {

	String key;
	ArrayList<String> values = new ArrayList<String>();
	
	/**
	 * Tokenize the line and store the key and add the value
	 */
	public KeyValues(String line) {
		String [] key_value = line.split("\t");
		key = key_value[0].trim();
		values.add(key_value[1].trim());
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public ArrayList<String> getValues() {
		return values;
	}
	public void setValues(ArrayList<String> values) {
		this.values = values;
	}
	
	public void addValue(String value) {
		values.add(value);
	}
	
	

}

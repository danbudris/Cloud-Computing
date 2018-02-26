package edu.bu.cs755.Task3;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable extends ArrayWritable {
	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	}
}
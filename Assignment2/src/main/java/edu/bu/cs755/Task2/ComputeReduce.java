package edu.bu.cs755.Task2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ComputeReduce extends Reducer<DoubleWritable, Text , Text, DoubleWritable> {
    
    public void reduce(DoubleWritable values, Iterable<Text> keyIn, Context context) throws IOException, InterruptedException {
    	
    	//System.out.println("\n reduce ");
    	for (Text key : keyIn) {
    		//System.out.println("key " + key + " value "+ values);
            context.write(key, values);
        }
    }
}
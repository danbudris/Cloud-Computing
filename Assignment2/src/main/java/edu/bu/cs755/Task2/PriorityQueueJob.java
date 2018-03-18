package edu.bu.cs755.Task2;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PriorityQueueJob extends Mapper<Object, Text, DoubleWritable, Text>{
	
	private static PriorityQueue<Map.Entry<Text,DoubleWritable>> worstFive = new PriorityQueue<Map.Entry<Text,DoubleWritable>>(
		    new Comparator<Map.Entry<Text, DoubleWritable>>() {
        	public int compare(Entry<Text, DoubleWritable> e1, Entry<Text, DoubleWritable> e2) {
        		return e1.getValue().compareTo(e2.getValue());
        }
    });
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {        	
    	String line = value.toString();
        String[] fields = line.split("\t");
        
        Text mapKey = new Text(fields[0]);
        DoubleWritable mapValue = new DoubleWritable(Double.parseDouble(fields[1]));
        
        Map<Text, DoubleWritable> mapper = new HashMap<>();
        
        mapper.put(mapKey, mapValue);
        
        worstFive.addAll(mapper.entrySet());
        
        if(worstFive.size() > 50){
        	worstFive.remove();
        }
        
    }
    
    public void cleanup(Context context) throws IOException, InterruptedException {
    	        	
    	//System.out.println(worstFive.size());
    	while(!worstFive.isEmpty()){
    		Map.Entry<Text, DoubleWritable> mapper = worstFive.poll();
    		//System.out.println(mapper);
    		
    		// context.write is sorted by key value.
    		context.write(mapper.getValue(), mapper.getKey());
    	}
    }
}




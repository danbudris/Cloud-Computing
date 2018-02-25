package edu.bu.cs755;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// successfully returns the error rate as a percentage of total rides
public class Task2 {
	    
    public static class GetMedallionErrors extends Mapper<Object, Text, Text, DoubleWritable>{

        // Set the variable which will be the value in the output map
        private final static DoubleWritable one = new DoubleWritable(1);
        private final static DoubleWritable zero = new DoubleWritable(0);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	
        	String line = value.toString();
            String[] fields = line.split(",");
            
            // only process records with exactly 17 fields, thus discarding some malformed records
            if  (fields.length == 17) {
                // if the record contains GPS errors (blank fields or all zeros), set the value to 1
            	// fields 6 - 9 are pickup/dropoff longitude and latitude
                if (fields[6].equals("0.000000") || fields[7].equals("0.000000") || fields[8].equals("0.000000") || fields[9].equals("0.000000") || 
                	fields[6].equals("") || fields[7].equals("") || fields[8].equals("") || fields[9].equals("")) {
                    context.write(new Text(fields[0]), one);
                }
                // if it does not have errors, set the value to 0
                else {
                    context.write(new Text(fields[0]), zero);
                }
            }
        }
    }

    public static class ErrRatePercentageReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            
            // Store the total number of records
            double totalSum = 0;
            // Store the number of error records
            double errSum = 0;
            double answer = 0;
            
            for (DoubleWritable val : values) {
                // increment the total number of trips this taxi has taken
                totalSum += 1;
                // increment the error counter; non-errors are equal to 0, errors equal to 1
                errSum += val.get();

            }
            // set the result to the percentage of error records in the total records for the given medallion number
            answer = errSum/totalSum;

            if(answer != 0){
            	result.set(answer);
            	context.write(key, result);
            }
        }
    }

    
    
    public static class PriorityQueueJob extends Mapper<Object, Text, Text, DoubleWritable>{
    	
    	private static PriorityQueue<Map.Entry<Text,DoubleWritable>> worstFive = new PriorityQueue<Map.Entry<Text,DoubleWritable>>(
    		    new Comparator<Map.Entry<Text, DoubleWritable>>() {
            	public int compare(Entry<Text, DoubleWritable> e1, Entry<Text, DoubleWritable> e2) {
            		return e2.getValue().compareTo(e1.getValue());
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
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
        	        	
        	System.out.println(worstFive.size());

        	//new AbstractMap.SimpleEntry<String, Integer>("exmpleString", 42);
        
        	while(!worstFive.isEmpty()){
        		Map.Entry<Text, DoubleWritable> mapper = worstFive.poll();
        		System.out.println(mapper);
        		
        		//context.write(mapper.getKey(), mapper.getValue());
        	}
        }
    }

    public static class ComputeFive extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            
            // Store the total number of records
            double totalSum = 0;
            // Store the number of error records
            double errSum = 0;
            double answer = 0;
            
            for (DoubleWritable val : values) {
                // increment the total number of trips this taxi has taken
                totalSum += 1;
                // increment the error counter; non-errors are equal to 0, errors equal to 1
                errSum += val.get();

            }
            // set the result to the percentage of error records in the total records for the given medallion number
            answer = errSum/totalSum;

            if(answer != 0){
            	result.set(answer);
            	context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {    	
    	// first job
    	Configuration conf = new Configuration();
    	
    	/*
        Job job =  new Job(conf, "task2-1");
        
        job.setJarByClass(Task2.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        // compute the fraction of errors for taxi. 
        job.setMapperClass(GetMedallionErrors.class);
        job.setCombinerClass(ErrRatePercentageReducer.class);
        job.setReducerClass(ErrRatePercentageReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        */
      
        // second job.
        
        conf = new Configuration();
        Job secondJob =  new Job(conf, "task2-2");
        
        secondJob.setJarByClass(Task2.class);
        
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(DoubleWritable.class);
        
        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(DoubleWritable.class);
        
        // compute the fraction of errors for taxi. 
        secondJob.setMapperClass(PriorityQueueJob.class);
        //secondJob.setCombinerClass(ComputeFive.class);
        //secondJob.setReducerClass(ComputeFive.class);
        
        
        // find five worst taxis.
        //job.setSortComparatorClass(org.apache.hadoop.io.DoubleWritable.Comparator.class);
        //job.setNumReduceTasks(5);// number of reduce tasks are set to 1
        
        FileInputFormat.addInputPath(secondJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(secondJob, new Path(args[2]));

        System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
    }
}
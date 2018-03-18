package edu.bu.cs755.Task3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.bu.cs755.Task2.ComputeReduce;
import edu.bu.cs755.Task3.PriorityQueueJob;

// Successfully returns the money-per-minute of all cab rides in data set
public class Task3 {

    public static class GetTripPrice extends Mapper<Object, Text, Text, DoubleArrayWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        // set the input string
	        String line = value.toString();
	        // split the string, on commas, into a list of strings
	        String[] fields = line.split(",");
	        // only process records with exactly 17 fields, the seconds and the fare are greater than 0, thus discarding some malformed records
	        
	     // get the total fare for the current ride
	        double fare = Double.parseDouble(fields[16]);
	        // create the var for output, and set to the dollars per minute for this ride
	        double time_sec =  Double.parseDouble(fields[4]);
	        
	        DoubleArrayWritable moneyPerSec = new DoubleArrayWritable();
	        DoubleWritable[] data = new DoubleWritable[2];
	        
	        if  (fields.length == 17 && fare > 0 && time_sec > 0) {
	            
	        	data[0] = new DoubleWritable(fare);
	        	data[1] = new DoubleWritable(time_sec);
	        	
	        	moneyPerSec.set(data);
	            
	            // write to the context the medallion number and money per minute for this ride
	            context.write(new Text(fields[0]), moneyPerSec);
	        }
        }
    }

    public static class SumFaresPerMinute extends Reducer<Text,DoubleArrayWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        
        public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
            
            double fare = 0;
            double time_sec = 0;
            
            Writable[] temp = new DoubleWritable[2];
            
            for (DoubleArrayWritable val : values) {
                // add to the total money per minute
            	temp = val.get();
            	fare += Double.parseDouble(temp[0].toString());
            	time_sec += Double.parseDouble(temp[1].toString());
            }
            
            // get time per min
            result.set(fare/(time_sec/60));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        
    	
    	// first job
    	Configuration conf = new Configuration();
    	
    	
        Job job =  new Job(conf, "task3-1");
        job.setJarByClass(Task3.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);
        
        job.setMapperClass(GetTripPrice.class);
        //job.setCombinerClass(SumFaresPerMinute.class);
        job.setReducerClass(SumFaresPerMinute.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        
        
        
    	
        // second job
        
        Job secondJob =  new Job(conf, "task3-2");
        secondJob.setJarByClass(Task3.class);
        
        // mapOutput is value : key because context.write sort by key.
        // this is setting for map out
        secondJob.setMapOutputKeyClass(DoubleWritable.class);
        secondJob.setMapOutputValueClass(Text.class);
        
        
        // compute the fraction of errors for taxi. 
        secondJob.setMapperClass(PriorityQueueJob.class);
        // don't need combiner.
        //secondJob.setCombinerClass();
        secondJob.setReducerClass(ComputeReduce.class);
        
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(secondJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(secondJob, new Path(args[2]));
        
        System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
    }
}
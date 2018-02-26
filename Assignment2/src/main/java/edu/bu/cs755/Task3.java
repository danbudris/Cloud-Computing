package edu.bu.cs755;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Successfully returns the money-per-minute of all cab rides in data set
public class Task3 {

    public static class GetTripPrice extends Mapper<Object, Text, Text, DoubleWritable>{

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
	            
	        if  (fields.length == 17 && fare > 0 && time_sec > 0) {
	            
	            DoubleWritable moneyPerSec = new DoubleWritable();
	            moneyPerSec.set(fare/time_sec);
	            
	            // write to the context the medallion number and money per minute for this ride
	            context.write(new Text(fields[0]), moneyPerSec);
	        }
        }
    }

    public static class SumFaresPerMinute extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            
            // the total money per minute for this medallion number
            double moneyPerMinute = 0;
            for (DoubleWritable val : values) {
                // add to the total money per minute
                moneyPerMinute += val.get();
            }
            // troubleshooting
            /*
            System.out.println("Total Rides:" + totalRides);
            System.out.println("Money Per Minute: " + moneyPerMinute);
            */

            // set the result to the money per minute total divided by the trips total, which is the average money per minute
            //result.set(moneyPerMinute/totalRides);
            //write to the context the medallion number and money per minute for this ride
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job =  new Job(conf, "task3");
        job.setJarByClass(Task2.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        job.setMapperClass(GetTripPrice.class);
        job.setCombinerClass(SumFaresPerMinute.class);
        job.setReducerClass(SumFaresPerMinute.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
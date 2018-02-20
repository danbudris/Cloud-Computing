package edu.bu.cs755;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import static java.util.Comparator.reverseOrder;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class StreamInput {

	private List<String> top5000;

	public StreamInput(){
		// Configure the log4j
		PropertyConfigurator.configure("log4j.properties");
		String bucket_name= "metcs755";
		String key_name="WikipediaPages_oneDocPerLine_1000Lines_small.txt";
		String big_key_name="WikipediaPages_oneDocPerLine_1m.txt"; 
		
		AmazonS3 s3Client = AmazonS3Client.builder().withRegion("us-east-1").build();	// this takes some time.

		try {
			
			// checking time
			long startTime = System.currentTimeMillis();
			
			// this is where it gets the text file from the web.
			
			S3Object s3object = s3Client.getObject(bucket_name, key_name);	// this takes some time.
			S3ObjectInputStream s3is = s3object.getObjectContent();
			BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
			Stream<String> streamlines = reader.lines().parallel();

		    long stopTime = System.currentTimeMillis();
		    long elapsedTime = stopTime - startTime;
		    System.out.println("read time :" + elapsedTime/1000);

		    top5000 = streamlines
					.map(line -> line.split(">")[1].replaceAll("<[^>]+>", "")) // 
					.flatMap(line -> Arrays.stream(line.trim().split(" ")))
					.map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim())
					.filter(word -> word.length() > 0)
					.collect(groupingBy(identity(), counting()))
					.entrySet().stream()
					.sorted(Map.Entry.<String, Long> comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
					.limit(5000)
					.map(Map.Entry::getKey)
					.collect(toList());
		    
		    System.out.println("count time :"+elapsedTime/1000);
		    
		    s3object.close();
		    s3is.close();
		    reader.close();
		    streamlines.close();
		    
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
	
	// for task 1
	public List<String> getTop5000List(){
		return top5000;
	}
}

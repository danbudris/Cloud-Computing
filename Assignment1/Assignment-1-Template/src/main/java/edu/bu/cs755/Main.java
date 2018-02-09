package edu.bu.cs755;

import java.util.List;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Main {

	 public static String extractText(String line){

		 return "";
	 }

	public static void main(String[] args) {

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
			
			S3Object s3object = s3Client.getObject(bucket_name, big_key_name);	// this takes some time.
			S3ObjectInputStream s3is = s3object.getObjectContent();
			
			// Some s3 file metadata printouts 			
		    System.out.println("Type " + s3object.getObjectMetadata().getContentType());
		    System.out.println("Length " + s3object.getObjectMetadata().getContentLength());

		    BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		    
		    Stream<String> lines=reader.lines().parallel();
		    //System.out.println("Number of Lines: "+ lines.count());		// this takes some time.
		    //lines.forEach(System.out::println);
		    
		    long stopTime = System.currentTimeMillis();
		    long elapsedTime = stopTime - startTime;
		    
		    System.out.println("read time :" + elapsedTime/1000);

		    startTime = System.currentTimeMillis();
		    
		    Map<String, Integer> wordCount=lines
		    		.map(line -> line.split(">")[1].replaceAll("<[^>]+>", "")) // 
		    		.flatMap(line -> Arrays.stream(line.trim().split(" ")))
		    		.map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim())
		    		.filter(word -> word.length() > 0)
		            .map(word -> new SimpleEntry<>(word, 1)) // create a tuple of (word, 1)
	            	//To solve the duplicated key issue above, pass in the third mergeFunction argument like this :
		            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (v1, v2) -> v1 + v2)); // do the actual reduce by adding values with the same keys 
		    
		    stopTime = System.currentTimeMillis();
		    elapsedTime = stopTime - startTime;
		    
		    System.out.println("count time "+elapsedTime/1000);
		    
		    // Now, we want to sort and get the top 50 values. 
		    Map<String, Integer> result = wordCount.entrySet().parallelStream()
		    			.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
		    			.limit(5000) // limit the output to 50 elements
		    			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));	// returns a LinkedHashMap, keep order
		    
		    
		    //result.forEach((k, v) -> System.out.println(String.format("%s ->  %d", k, v)));
		    //System.out.println(result.keySet());
		    
		    
		    
		    /**
		     * Task 1
		     */
		    System.out.println("\n**** TASK 1 ****");
		    
		    startTime = System.currentTimeMillis();
		    
		    List<String> positionArray = new ArrayList<String>(result.keySet());
		    Map<String, Integer> map = new HashMap<>();
		    Integer order = 1;
		    
		    for (String index : positionArray){
		        map.put(index, order++);
		    }
		     
		    //map.forEach((k, v) -> System.out.println(String.format("%s ->  %d", k, v)));
		    //System.out.println(map.keySet());		    
		    
		    String [] inputtest = {"during", "and", "time", "protein", "car"};
		    for(String input : inputtest){
		    	if( map.containsKey(input)){
		    		System.out.println(String.format(input + " -> %d", map.get(input) ));
		    	}
		    	else{
		    		System.out.println(String.format(input + " -> -1"));
		    	}
		    }
		    
		    stopTime = System.currentTimeMillis();
		    elapsedTime = stopTime - startTime;
		    System.out.println("index time "+elapsedTime/1000);
	    
			s3is.close();
			
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

}

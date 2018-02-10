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

	public static void main(String[] args) throws IOException {

		StreamInput readInput = new StreamInput();
		
		
		/*
		StreamInput readInput = new StreamInput();
		
		Stream<String> totalLines = readInput.getTotalLine();
		Map<String, Integer> Wordcount = readInput.getWordCount();
		Map<String, Integer> top5000 = readInput.Gettop5000WordsMap();
		
		
		
		Task1 task1 = new Task1();
		Task2 task2 = new Task2();
		
		task1.frequencyPosition(top5000);
		
		task2.ranklist(totalLines, Wordcount);
	 	*/
		/*
		Task2 task2 = new Task2();
		task2.test();
	*/
	}
}

package edu.bu.cs755;

import static java.util.Comparator.reverseOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class Task2 {
	
	private Stream<String> lines;
	
	List<String> Fivethousandwords;
	List<String> TopList20;
	
	public Task2 (){
		
	}
	
	public void ranklist(Stream<String> totalLines, Map<String, Integer> wordcount){
		
		/**
	     * Task 2
	     */
	    System.out.println("\n**** TASK 2 ****");
	    
	    
	}
	
	
	
	
	public void test(){
		String testInput = "<doc id=\"432019\" url=\"https://en.wikipedia.org/wiki?curid=432019\" title=\"Fresno scraper\">Fresno scraperThe Fresno Scraper is a machine pulled by horses used for constructing canals and ditches in sandy soil. The design of the Fresno Scraper forms the basis of most modern earthmoving scrapers, having the ability to scrape and move a quantity of soil, and also to discharge it at a controlled depth, thus quadrupling the volume which could be handled manually.The Fresno scraper was invented in 1883 by James Porteous. Working with farmers in Fresno, California, he had recognised the dependence of the Central San Joaquin Valley on irrigation, and the need for a more efficient means of constructing canals and ditches in the sandy soil. In perfecting the design of his machine, Porteous made several revisions on his own and also traded ideas with William Deidrick, Frank Dusy, and Abijah McCall, who invented and held patents on similar scrapers. Porteous bought the patents held by Deidrick, Dusy, and McCall, gaining sole rights to the Fresno Scraper.Prior scrapers pushed the soil ahead of them, while the Fresno scraper lifted it into a C-shaped bowl where it could be dragged along with much less friction. By lifting the handle, the operator could cause the scraper to bite deeper. Once soil was gathered, the handle could be lowered to raise the blade off the ground so it could be dragged to a low spot, and dumped by raising the handle very high.This design was so revolutionary and economical that it has influenced the design of modern bulldozer blades and earth movers to this day.Between 1884 and 1910 thousands of Fresno scrapers were produced at the Fresno Agricultural Works which had been formed by Porteous, and used in agriculture and land levelling, as well as road and railroad grading and the construction industry. They played a vital role in the construction of the Panama Canal and later served the US Army in World War I.It was one of the most important agricultural and civil engineering machines ever made. In 1991 the Fresno Scraper was designated as an International Historic Engineering Landmark by the American Society of Mechanical Engineers. It is currently on display at the San Joaquin County Historical Society & Museum.</doc>\n"
				+ "<doc id=\"432067\" url=\"https://en.wikipedia.org/wiki?curid=432067\" title=\"Richard Rich (disambiguation)\">Richard Rich (disambiguation)Richard Rich, 1st Baron Rich (1496/7¡°1567) was Lord Chancellor of England.Richard Rich may also refer to:</doc>\n"
				+ "<doc id=\"432020\" url=\"https://en.wikipedia.org/wiki?curid=432019\" title=\"Fresno scraper\">Fresno scraperThe Fresno Scraper is a machine pulled by horses used for constructing canals and ditches in sandy soil. The design of the Fresno Scraper forms the basis of most modern earthmoving scrapers, having the ability to scrape and move a quantity of soil, and also to discharge it at a controlled depth, thus quadrupling the volume which could be handled manually.The Fresno scraper was invented in 1883 by James Porteous. Working with farmers in Fresno, California, he had recognised the dependence of the Central San Joaquin Valley on irrigation, and the need for a more efficient means of constructing canals and ditches in the sandy soil. In perfecting the design of his machine, Porteous made several revisions on his own and also traded ideas with William Deidrick, Frank Dusy, and Abijah McCall, who invented and held patents on similar scrapers. Porteous bought the patents held by Deidrick, Dusy, and McCall, gaining sole rights to the Fresno Scraper.Prior scrapers pushed the soil ahead of them, while the Fresno scraper lifted it into a C-shaped bowl where it could be dragged along with much less friction. By lifting the handle, the operator could cause the scraper to bite deeper. Once soil was gathered, the handle could be lowered to raise the blade off the ground so it could be dragged to a low spot, and dumped by raising the handle very high.This design was so revolutionary and economical that it has influenced the design of modern bulldozer blades and earth movers to this day.Between 1884 and 1910 thousands of Fresno scrapers were produced at the Fresno Agricultural Works which had been formed by Porteous, and used in agriculture and land levelling, as well as road and railroad grading and the construction industry. They played a vital role in the construction of the Panama Canal and later served the US Army in World War I.It was one of the most important agricultural and civil engineering machines ever made. In 1991 the Fresno Scraper was designated as an International Historic Engineering Landmark by the American Society of Mechanical Engineers. It is currently on display at the San Joaquin County Historical Society & Museum.</doc>\n";		
	    
		List<String> splitString = Arrays.asList(testInput.split(">\n"));

		// word list
		Fivethousandwords = new ArrayList<String>();
		Fivethousandwords.add("the");
		Fivethousandwords.add("is");
		Fivethousandwords.add("refer");
		
		Map<String , Integer> donyoo = splitString.stream()
				.limit(20)		// limit top 20
				.sorted()
				.collect(Collectors.toMap(e ->getTheKey(e), v -> getTheValue(v) ));
		
		
		System.out.println("\n\n\n");
		System.out.println(donyoo);

	}

	private String getTheKey(String e) {

		return e.substring(9, 15);
	}

	private Integer getTheValue(String v) {
		
	    List<String> words = Arrays.asList(v.split(" "));

	    // get only words from the 5000 list.
	    Predicate<String> predicate =
	    		e -> (Fivethousandwords.contains(e));
	    
		Long resultvalue = 
	            words.stream()
	            	 .map(line -> line.replaceAll("<[^>]+>", ""))
	            	 .flatMap(line -> Arrays.stream(line.trim().split(" ")))	
	                 .map(String::toLowerCase)
	                 .filter(word -> word.length() > 0)
	                 .filter(predicate)
	                 
	                 .collect(groupingBy(identity(), counting()))
	                 .entrySet().stream()
	                 .sorted(Map.Entry.<String, Long> comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
	                 .limit(5000)
	                 .map(e ->e.getValue())
	                 .reduce((long) 0, (x,y) -> x +y);
	                 //.collect(toList());

	    System.out.println("final list : " + resultvalue);

	    return resultvalue.intValue();
	}
}

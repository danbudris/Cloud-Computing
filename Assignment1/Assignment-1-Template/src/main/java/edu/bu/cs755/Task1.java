package edu.bu.cs755;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Task1 {
	
	public Task1(){
		
	}
	
	public void frequencyPosition(List<String> Top5000) {
		
		 /**
	     * Task 1
	     */
	    System.out.println("\n**** TASK 1 ****");

	    Map<String, Integer> map = new HashMap<>();
	    Integer order = 1;

	    for (String index : Top5000){
	        map.put(index, order++);
	    }

	    String [] inputtest = {"during", "and", "time", "protein", "car"};
	    for(String input : inputtest){
	    	if( map.containsKey(input)){
	    		System.out.println(String.format(input + " -> %d", map.get(input) ));
	    	}
	    	else{
	    		System.out.println(String.format(input + " -> -1"));
	    	}
	    }

	}

}

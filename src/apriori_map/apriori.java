package apriori_map;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.*;



public class apriori {
	
	public static String inputFile = "data_for_project.txt";
	public static String outputFolder = "mapreduce";
	public static String outputFile = "./" + outputFolder + "/part-r-00000";
	public static Map<String, Integer> frequentItems;
	public static List<ArrayList<String>> candidatePairs;
	public static Map<ArrayList<String>, Integer> frequentPairs;
	public static int totalBaskets, numOfItems;
	public static double confidenceThreshold = 2;
	public static double supportThreshold = 0.01;
	public static int passNumber = 1;
	public static int total_read[] = new int[10];
	public static String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
	public static ArrayList<String> frequent_items = new ArrayList<String>();
    
	private static void gettotalBaskets() {
		try {
			 //System.err.println("Output file: " + apriori.outputFile ); 
			BufferedReader bReader = new BufferedReader(new FileReader(outputFile));
			String line;
			
			while ((line = bReader.readLine()) != null) {
				String toAdd = new String();
				toAdd = line.split("\\t")[0];
				frequent_items.add(toAdd);
			}
			bReader.close();
		}
		catch (Exception e) {
			System.out.println("Error reading input file.");
		}
	}
    
	private static boolean areItemsInBasket(ArrayList<String> items, String[] basket) {
		boolean itemsPresent = true, isPresent;
		
		for(int i=0; i<items.size(); i++) {
			isPresent = false; //Assume item is not present in basket
			for(int j=0; j<basket.length; j++) {
				if(items.get(i).equals(basket[j]))
					isPresent = true;
			}
			if(isPresent == false) 
				itemsPresent = false; //If item is not present, that means the item set is also not present in the basket			
		}
		
		return itemsPresent;
	}
	/*
	private static void findFrequentPairs() {
		frequentPairs = new HashMap<ArrayList<String>, Integer>();
		
		try {
			String line;
			String[] basket;

			for(int i=0; i<candidatePairs.size(); i++) { //Check each candidate pair
				int count = 0;
				
				BufferedReader bReader = new BufferedReader(new FileReader(inputFile));
				//Go through input file line by line
				while ((line = bReader.readLine()) != null) {
					basket = line.split(",");					
					if(areItemsInBasket(candidatePairs.get(i), basket)) { //If candidate pair is in the item set/basket
						count = count+1;
					}					
				}
				bReader.close();
				
				if(checkSupport(count) == true)
					frequentPairs.put(candidatePairs.get(i), count); //add pair if it is frequent
			}		
		}
		catch (Exception e) {
			System.out.println("Error reading map reduce file");
		}
			
		System.out.println("Frequent Pairs = " + frequentPairs);
	}*/
	
	// Create candidate set using item numbers of frequent items
	private static void getCandidatePairs() {
		candidatePairs = new ArrayList<ArrayList<String>>(); // a list of lists
		ArrayList<String> pair = new ArrayList<String>();

		for (String key1 : frequentItems.keySet()) {
			for (String key2 : frequentItems.keySet()) {
				if(!key1.equals(key2)) {
					pair.add(key1);
					pair.add(key2);
					candidatePairs.add(pair);
					pair = new ArrayList<String>();
				}
			}	
		}			
		System.out.println("Candidate Pairs = " + candidatePairs);
	}

	
	

	
	public static boolean checkSupport(int sum, int total) {
		boolean isFrequent;
		double s = 1.0 * sum/total;
		if(s >=  apriori.supportThreshold)
			isFrequent = true;
		else
			isFrequent = false;
		
		return isFrequent;
		
		
	}
	/*
	private static void findFrequentItems() {
		
		frequentItems = new HashMap<String, Integer>();
		try {
			BufferedReader bReader = new BufferedReader(new FileReader(outputFile));
			String line;
			String[] item;
			//Find single item sets, Pass-1
			numOfItems = 0;
			while ((line = bReader.readLine()) != null) {
				item = line.split("\t");
				int num = Integer.parseInt(item[1]);
				
				if(checkSupport(num) == true) { //Add frequent item sets
					frequentItems.put(item[0], num);
				}
				numOfItems++;
			}

			System.out.println("Frequent items = " + frequentItems.size() + " Items = " + numOfItems );
			System.out.println("Frequent Items = " + frequentItems);
			bReader.close();
		}
		catch (Exception e) {
			System.out.println("Error reading map reduce file");
		}
	}*/
	
	public static boolean createJob(String input, String output,Object aprioriMapper, Object aprioriReducer,  Configuration conf){
		boolean success = false;
		
		try {
	        @SuppressWarnings("deprecation")
	       Job job = new Job(conf, "aprior");
	       job.setJarByClass(apriori.class); //Tell hadoop the name of the class every cluster has to look for
	       
	       job.setMapperClass(aprioriMapper.class); //Set class that will be executed by the mapper
	       job.setReducerClass(aprioriReducer.class); //Set the class that will be executed as the reducer
	      
	       job.setOutputKeyClass(Text.class); //Set the class to be used as the key for outputting data to the user
	       job.setOutputValueClass(IntWritable.class); //Set class that will be used as the vaue for outputting data
	     
	       FileInputFormat.addInputPath(job,  new Path(input)); //Get input file name
	       FileOutputFormat.setOutputPath(job, new Path(output));
		
	       success = job.waitForCompletion(true);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	       return success;
	}
	
	public static boolean createJob2(String input, String output,Object aprioriMapper, Object aprioriReducer,  Configuration conf){
		boolean success = false;
		
		try {
	        @SuppressWarnings("deprecation")
	       Job job = new Job(conf, "aprior");
	       job.setJarByClass(apriori.class); //Tell hadoop the name of the class every cluster has to look for
	       
	       job.setMapperClass(aprioriMapper3.class); //Set class that will be executed by the mapper
	       job.setReducerClass(aprioriReducer3.class); //Set the class that will be executed as the reducer
	      
	       job.setOutputKeyClass(Text.class); //Set the class to be used as the key for outputting data to the user
	       job.setOutputValueClass(IntWritable.class); //Set class that will be used as the vaue for outputting data
	     
	       FileInputFormat.addInputPath(job,  new Path(input)); //Get input file name
	       FileOutputFormat.setOutputPath(job, new Path(output));
		
	       success = job.waitForCompletion(true);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	       return success;
	}
	public static void main(String[] args) throws InterruptedException, ClassNotFoundException {

		
//		gettotalBaskets();
//		findFrequentItems();
//		getCandidatePairs();
//		findFrequentPairs();

		

		try {
		Configuration conf = new Configuration();
        String[] otherArgs;
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
        if (otherArgs.length != 2) {
                System.err.println("Usage: aPriori <in> <out>");
                System.exit(2);
        }
        
        //Setup the initial variables
        System.err.println("Input: " + otherArgs[0]);
        System.err.println("Output: " + otherArgs[1]);
        String inputDir = otherArgs[0];
        String outputTempDir1 = otherArgs[1] + "/" + timeStamp + "/Temp/1";
        String outputTempDir2 = otherArgs[1] + "/" + timeStamp + "/Temp/2"; 
        outputFile = "./"  + outputTempDir1 + "/part-r-00000";
        //Start new job to get frequent items
        boolean success = createJob(inputDir, outputTempDir1,aprioriMapper.class,aprioriReducer.class,conf);
        if(success == true){
           System.err.println("Job finished succesfully");
        }
        gettotalBaskets();

        System.out.println("===================Frequent Items=================" + frequent_items);
      //Start new job to get frequent items
        boolean success2 = createJob2(inputDir, outputTempDir2,aprioriMapper3.class,aprioriReducer.class,conf);
        if(success2 == true){
     	   System.err.println("Job finished succesfully");
        }
     
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
}
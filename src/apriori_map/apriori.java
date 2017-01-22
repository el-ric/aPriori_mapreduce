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
import java.util.*;

public class apriori {
	// USER INPUT (Apriori Variables)
	public static double confidence = 0.4;
	public static int supportFrequency = 100;
	//public static double supportThreshold = 0.1; // Frequency as a fraction
		
		
	// INPUT-OUTPUT FOLDERS
	public static String inputFile = "data_for_project.txt";
	public static String outputFolder = "mapreduce";
	
	// GLOBAL VARIABLES
	public static String outputFile;
	public static int total_read[] = new int[50];
	public static int passNumber;
	public static String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
	
	// DATA STRUCTURES
	public static ArrayList<String> freqItems, candidateItems;
	public static ArrayList<Map<String, Integer>> allFrequentItemsets = new ArrayList<Map<String, Integer>>();
	public static ArrayList<String[]> associationRules = new ArrayList<String[]>(); //Format: (String[0] = A, String[1] = B, String[2] = C) => A -> B, confidence C
    
	private static boolean isItemInBasket(String items, String basket) {
		String[] itemSet = items.split(",");
		String[] basketSet = basket.split(",");
		boolean isPresent = true;
		
		for(int i=0; i<itemSet.length; i++) {
			boolean inBasket = false;
			for(int j=0; j<basketSet.length; j++) {
				if(itemSet[i].equals(basketSet[j]))
					inBasket = true;
			}
			if(!inBasket)
				isPresent = false;
		}
		
		return isPresent;
	}
	
	private static void readFrequentItemsFromFile(String filename) {
		freqItems = new ArrayList<String>();
		Map<String, Integer> freqItemset = new HashMap<String, Integer>();
		
		try {
			BufferedReader bReader = new BufferedReader(new FileReader(filename));
			String line;
			
			while ((line = bReader.readLine()) != null) {
				String itemName = new String();
				Integer itemFreq;
				itemName = line.split("\\t")[0];
				itemFreq = Integer.valueOf(line.split("\\t")[1]);
				freqItems.add(itemName);
				freqItemset.put(itemName, itemFreq);
			}
			bReader.close();
		}
		catch (Exception e) {
			System.out.println("Error reading input file.");
		}

		allFrequentItemsets.add(freqItemset);
	}
	
	private static String combineStrings(String[] strings) {
		Arrays.sort(strings);
		Arrays.stream(strings).distinct();
		return String.join(",", strings);
	}
	
	private static void getCandidateItems() {
		candidateItems = new ArrayList<String>();
		for(int i=0; i<freqItems.size(); i++) {
			String line = freqItems.get(i);
			String[] items = line.split(",");
			String firstWord = items[0];
			String subString = combineStrings(Arrays.copyOfRange(items, 1, items.length));
			
			for(int j=i+1; j<freqItems.size(); j++) {
				String tempItemset = freqItems.get(j);
				if(isItemInBasket(subString, tempItemset)) {
					String lastWord = tempItemset.split(",")[items.length-1];
					
					//Check if first and last word is in the code
					Boolean isCandidate = false;
					for(int k=0; k<freqItems.size(); k++) {	
						if(isItemInBasket(firstWord, freqItems.get(k)) && isItemInBasket(lastWord,freqItems.get(k))) {
							isCandidate = true;
						}
					}
					
					if(isCandidate) {
						String[] newSet = {firstWord,tempItemset};
						String newCandidate = combineStrings(newSet);
						if(!candidateItems.contains(newCandidate)) {
							candidateItems.add(newCandidate);
						}
					}
				}
			}
		}
	}
	
	
	public static boolean checkFrequency(int sum, int total) {
		/*		
		double s = 1.0 * sum/total;
		if(s >=  apriori.supportThreshold)
			return true;
		else
			return false;
		*/
		
		if (sum>=supportFrequency)
			return true;
		else
			return false;
	}
	
	private static void getFrequentPairs(String filename) {
		candidateItems = new ArrayList<String>();
		for(int i=0; i<freqItems.size(); i++)
			for(int j=i+1; j<freqItems.size(); j++) {
				String line=freqItems.get(i) + "," + freqItems.get(j);
				candidateItems.add(line);
			}
	}
	
	private static void findFrequentItems() {
		try {
			Configuration conf = new Configuration();
			
			//Find Frequent Items
			passNumber = 1;
			String outputDir = outputFolder + "/" + timeStamp + "/" + passNumber;
			boolean success = createJob(inputFile, outputDir,"apriori_map.aprioriMapper","apriori_map.aprioriReducer",conf);
			outputFile = "./"  + outputDir + "/part-r-00000";
			if(success == true){
		           System.err.println("\nPass 1 Completed Succesfully\n");
		    }
			readFrequentItemsFromFile(outputFile);
			getCandidateItems();
			
			//Find k-item sets
			passNumber = 2;
			getFrequentPairs(outputFile);
			while(candidateItems.size() > 0) {
				//Map Reduce
				outputDir = outputFolder + "/" + timeStamp + "/" + passNumber;	
				success = createJob(inputFile, outputDir,"apriori_map.aprioriMapper","apriori_map.aprioriReducer",conf);
				if(success == true)
			        System.err.println("\nPass " + passNumber + " Completed Succesfully\n");
				else 
					System.err.println("\nPass " + passNumber + " Failed \n");
				
				//Read new data
				outputFile = "./"  + outputDir + "/part-r-00000";
				outputFile = "./"  + outputDir + "/part-r-00000";
				readFrequentItemsFromFile(outputFile);
				getCandidateItems();
				
				passNumber += 1;
			}   
		} 
		catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	public static boolean createJob(String input, String output,String aprioriMapper, String aprioriReducer,  Configuration conf){
		boolean success = false;
		
		try {
	        @SuppressWarnings("deprecation")
	       Job job = new Job(conf, "aprior");
	       job.setJarByClass(apriori.class); //Tell hadoop the name of the class every cluster has to look for
	       Class cls_mapper = Class.forName(aprioriMapper);
	       Class cls_reducer = Class.forName(aprioriReducer);
	       job.setMapperClass(cls_mapper); //Set class that will be executed by the mapper
	       job.setReducerClass(cls_reducer); //Set the class that will be executed as the reducer
	       job.setOutputKeyClass(Text.class); //Set the class to be used as the key for outputting data to the user
	       job.setOutputValueClass(IntWritable.class); //Set class that will be used as the vaue for outputting data
	       FileInputFormat.addInputPath(job,  new Path(input)); //Get input file name
	       FileOutputFormat.setOutputPath(job, new Path(output));
	       success = job.waitForCompletion(true);
		} 
		catch (IOException | ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		} 

		return success;
	}

	private static String getAssociation(String itemset, String basket) {
		String[] items  = itemset.split(",");
		String[] basketItems = basket.split(",");
		String remainingItems = new String();
		
		remainingItems = "";
		for(int i=0; i<basketItems.length; i++) {
			boolean inItemset = false;
			for(int j=0; j<items.length; j++) {
				if(basketItems[i].equals(items[j]))
					inItemset = true;
			}
			if(!inItemset) {
				if(remainingItems.equals(""))
					remainingItems = basketItems[i];
				else
					remainingItems = remainingItems + "," + basketItems[i];
			}
		}
	
		remainingItems = combineStrings(remainingItems.split(",")); // Sort string in ascending order
		
		return remainingItems;
	}
	
	// Returns all combinations in an array list of 2D Strings containing left and right part of association rules
	public static ArrayList<String[]> getCombinations(String string, int size) {
		ArrayList<String[]> assRules = new ArrayList<String[]>();
		String[] items  = string.split(",");
		
		for (int i=0; i+size<=items.length; i++) {
			for(int j=i+1; j<items.length; j++) {
				String comb = new String();
				comb = items[i];
				for(int k=0; k<size-1; k++)
					if(j+k < items.length) {
						comb = comb + "," + items[j+k];
					}
					else if(i+k+size <= items.length) {
						comb = comb + "," + items[k];
					}
				if(comb.length() == 2*size-1) {
					String associatedItems = getAssociation(comb, string);
					String[] association = new String[2];
					association[0] = comb;
					association[1] = associatedItems;
					assRules.add(association);
				}
			}
		}
		return assRules;
	}
	
	private static void findRulesForPairs() {
		Map itemset = new HashMap<String, Integer>();
		Map freqItems = new HashMap<String, Integer>();
		itemset = allFrequentItemsets.get(1);
		freqItems = allFrequentItemsets.get(0);
		
	    Iterator it = itemset.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        String[] items = pair.getKey().toString().split(",");
	        
	        int supportAandB = pair.getValue().hashCode();
	        int supportA = freqItems.get(items[0]).hashCode();

	        double c = 1.0 * supportAandB/supportA;
	        
	        //Add rule if it meets confidence criteria
	        if(c>=confidence)
	        	addRule(items[0], items[1], c);
	        
	        // Check reverse association
	        supportA = freqItems.get(items[1]).hashCode();
	        c = 1.0 * supportAandB/supportA;
	        
	        if(c>=confidence)
	        	addRule(items[1], items[0], c);

	        it.remove(); // avoids a ConcurrentModificationException
	    }
			
	}
	
	private static void addRule(String a, String b, double c) {
		String[] rule = new String[3];
    	rule[0] = a;
    	rule[1] = b;
    	rule[2] = Double.toString(c);
    	associationRules.add(rule);	 
	}
	
	private static void findRulesForTriples() {
		Map itemset = new HashMap<String, Integer>();
		Map freqPairs = new HashMap<String, Integer>();
		itemset = allFrequentItemsets.get(2);
		freqPairs = allFrequentItemsets.get(1);		
		
		Iterator it = itemset.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry line = (Map.Entry)it.next();
	        String triple = line.getKey().toString();
	        ArrayList<String[]> potentialRules = getCombinations(triple, 2);
	        
	        for(int i=0; i<potentialRules.size(); i++) {
	        	String a = potentialRules.get(i)[0];
	        	String b = potentialRules.get(i)[0];
	        	int supportAandB = line.getValue().hashCode();
		        int supportA = freqPairs.get(a).hashCode();
		        double c = 1.0 * supportAandB/supportA;
		        System.out.println("Item: " + a + " -> " + b + "Sup A and B " + supportAandB + " Support A " + supportA);
		        if(c>=confidence)
		        	addRule(a, b, c);
	        }
	    }
	}
	
	public static void displayAssociationRules() {
		System.out.println("\n==== Association Rules ====\n");
		for(int i=0; i<associationRules.size(); i++) {
			String[] rule = associationRules.get(i);
			System.out.println(rule[0] + " -> " + rule[1] + "\t" + rule[2]);
		}
	}
	
	private static void findAssociationRules() {
		if(allFrequentItemsets.size() > 1)
			findRulesForPairs();
		if(allFrequentItemsets.size() > 2)
			findRulesForTriples();
	}
	
	public static void main(String[] args) throws InterruptedException, ClassNotFoundException {
		findFrequentItems();
		findAssociationRules();
		displayAssociationRules();
	}
}
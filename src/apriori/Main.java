package apriori;
import wordcount.WordCount;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Main {
	public static String inputFile = "data_for_project.txt";
	public static String outputFolder = "frequentItems";
	public static String outputFile = "./" + outputFolder + "/part-r-00000";
	public static Map<String, Integer> frequentItems, itemNumber;
	public static Map<Integer, String> items;
	public static int[] freqItems;
	public static List candidatePairs;
	public static int totalItemsets;
	public static double confidenceThreshold = 2;
	public static double supportThreshold = 0.05;
	public static int passNumber = 1;
	
	// Create candidate set
	private static void getCandidatePairs() {
		candidatePairs = new ArrayList<ArrayList>(); // a list of lists
		List<Integer> pair = new ArrayList<Integer>();
		pair.add(1);
		pair.add(2);
		candidatePairs.add(pair);
		candidatePairs.add(pair);
		candidatePairs.add(pair);
	

		System.out.println(candidatePairs);
	}
		
	// Gets the frequent item set of item numbers
	private static void mapFrequentItems() {
		int numFreqItems = frequentItems.size();
		freqItems = new int[numFreqItems];
		int i=0;
		for (String key : frequentItems.keySet()) {
			 freqItems[i] = getItemNumber(key);
			 i++;
		}
	}
	
	// Accepts item number, returns item name
	public static String getItemName(int itemNum) {
		return items.get(itemNum);
	}
	
	// Accepts item name, returns item number
	public static int getItemNumber(String itemName) {
		return itemNumber.get(itemName);
	}
	
	private static void getTotalItemsets() {
		try {
			BufferedReader bReader = new BufferedReader(new FileReader(inputFile));
			int count = 0;
			while ((bReader.readLine()) != null) {
				count++;
			}
			totalItemsets = count;
			bReader.close();
		}
		catch (Exception e) {
			System.out.println("Error reading input file.");
		}
	}
	
	private static void performWordCount() {
		WordCount w = new WordCount();
		try {
			w.getWordCount(inputFile, outputFolder);
		}
		catch (Exception e) {
			System.out.println("There was an I/O error");
		}
	}
	
	private static void findFrequentItems() {
		performWordCount();
		itemNumber = new HashMap<String, Integer>();
		items = new HashMap<Integer, String>();
		frequentItems = new HashMap<String, Integer>();
		try {
			BufferedReader bReader = new BufferedReader(new FileReader(outputFile));
			String line;
			String[] item;
			int mapCount = 1;
			//Find single item sets, Pass-1
			while ((line = bReader.readLine()) != null) {
				item = line.split("\t");
				int num = Integer.parseInt(item[1]);
				double s = 1.0 * num/totalItemsets;
				itemNumber.put(item[0], mapCount);
				items.put(mapCount, item[0]);

				if(s >= supportThreshold) { //Add frequent item sets
					frequentItems.put(item[0], num);
					
				}
				mapCount++;
			}
			//System.out.println(items);
			System.out.println(frequentItems);
			System.out.println("Frequent items = " + frequentItems.size() + " Items = " + items.size());
			bReader.close();
		}
		catch (Exception e) {
			System.out.println("Error reading map reduce file");
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		getTotalItemsets();
		findFrequentItems();
		mapFrequentItems();
		getCandidatePairs();
	}

}
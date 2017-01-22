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
	public static String outputFile;
	public static double supportThreshold = 0.1;
	public static int supportFrequency = 80;
	public static int total_read[] = new int[50];
	public static int passNumber;
	public static String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
	public static ArrayList<String> freqItems, candidateItems;
	public static ArrayList<Map<String, Integer>> allFrequentItemsets = new ArrayList<Map<String, Integer>>();
    
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
	
	private static int checkResults(String string) {
		int count=0;
		try {
			BufferedReader bReader = new BufferedReader(new FileReader(inputFile));
			String line;
			while ((line = bReader.readLine()) != null) {
				String[] items = string.split(",");
				
				Boolean flag = true;
				for(int k=0; k<items.length; k++) {	
					if(!isItemInBasket(items[k], line))
						flag = false;
				}
				
				if(flag)
					count += 1;
			}
			bReader.close();
		}
		catch (Exception e) {
			System.out.println("Error reading input file.");
		}
		return count;
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
		/*boolean isFrequent;
		
		double s = 1.0 * sum/total;
		if(s >=  apriori.supportThreshold)
			isFrequent = true;
		else
			isFrequent = false;
		
		
		return isFrequent;*/
		
		if (sum>supportFrequency)
			return true;
		else
			return false;
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
			
			passNumber = 1;
			String outputDir = outputFolder + "/" + timeStamp + "/" + passNumber;
			boolean success = createJob(inputFile, outputDir,"apriori_map.aprioriMapper","apriori_map.aprioriReducer",conf);
			outputFile = "./"  + outputDir + "/part-r-00000";
			if(success == true){
		           System.err.println("\nPass 1 Completed Succesfully\n");
		    }
			readFrequentItemsFromFile(outputFile);
			getCandidateItems();
			
			passNumber = 2;
			getFrequentPairs(outputFile);
			while(candidateItems.size() > 0) {
				outputDir = outputFolder + "/" + timeStamp + "/" + passNumber;
				
				success = createJob(inputFile, outputDir,"apriori_map.aprioriMapper","apriori_map.aprioriReducer",conf);
				outputFile = "./"  + outputDir + "/part-r-00000";
				if(success == true) {
			        System.err.println("\nPass " + passNumber + " Completed Succesfully\n");
			    }
				else {
					System.err.println("\nPass " + passNumber + " Failed \n");
				}
				
				outputFile = "./"  + outputDir + "/part-r-00000";
				readFrequentItemsFromFile(outputFile);
				getCandidateItems();
				
				passNumber += 1;
			}
			     
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) throws InterruptedException, ClassNotFoundException {

		findFrequentItems();
		//System.out.println(checkResults("butter,other vegetables,whole milk"));
	}
}
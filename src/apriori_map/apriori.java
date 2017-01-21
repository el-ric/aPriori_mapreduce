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
	public static int supportFrequency = 70;
	public static int total_read[] = new int[50];
	public static int passNumber;
	public static String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
	public static ArrayList<String> freqItems, candidateItems;
	public static ArrayList<Map<String, Integer>> allFrequentItemsets = new ArrayList<Map<String, Integer>>();
    
	private static void readFrequentItemsFromFile(String filename) {
		freqItems = new ArrayList<String>();
		Map<String, Integer> freqItemset = new HashMap<String, Integer>();
		
		try {
			 //System.err.println("Output file: " + apriori.outputFile ); 
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
		String combinedString = "";
		
		for(int i=0; i<strings.length;i++) {
			if(i==0)
				combinedString += strings[i];
			else
				combinedString += "," + strings[i];
		}
		
		return combinedString;
	}
	
	private static void getCandidateItems() {
		candidateItems = new ArrayList<String>();
		for(int i=0; i<freqItems.size(); i++) {
			String line = freqItems.get(i);
			String[] items = line.split(",");
			String firstWord = items[0];
			String subString = combineStrings(Arrays.copyOfRange(items, 1, items.length));
			
			for(int j=i+1; j<freqItems.size(); j++) {
				if(freqItems.get(j).contains(subString)) {
						
					//Check 3rd condition - do later. For now, assume candidate pair
					String lastWord = freqItems.get(j).split(",")[items.length-1];
					
					//Check if first and last word is in the code
					Boolean isCandidate = false;
					for(int k=0; k<freqItems.size(); k++) {	
						if(freqItems.get(k).contains(firstWord) && freqItems.get(k).contains(lastWord)) {
							isCandidate = true;
						}
					}
					if(isCandidate) {
						String[] newSet = {firstWord,freqItems.get(j)};
						String newCandidate = combineStrings(newSet);
						if(!candidateItems.contains(newCandidate)) {
							candidateItems.add(newCandidate);
						}
					}
				}
			}
		}
	}
	
	
	public static boolean checkSupport(int sum, int total) {
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
			// TODO Auto-generated catch block
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
	
	public static void main(String[] args) throws InterruptedException, ClassNotFoundException {

		try {
		Configuration conf = new Configuration();
		
        String[] otherArgs;
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
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
		outputDir = outputFolder + "/" + timeStamp + "/" + passNumber;
		outputFile = "./"  + outputDir + "/part-r-00000";
		success = createJob(inputFile, outputDir,"apriori_map.aprioriMapper3","apriori_map.aprioriReducer3",conf);
		if(success == true){
	           System.err.println("\nPass 2 Completed Succesfully\n");
	    }
		readFrequentItemsFromFile(outputFile);
		//System.out.println("Frequent Items " + freqItems);
		getCandidateItems();
		//System.out.println("\n============== Candidate Items ============\n " + candidateItems);
		
		passNumber = 3;
		while(candidateItems.size() > 0) {
			outputDir = outputFolder + "/" + timeStamp + "/" + passNumber;
			
			success = createJob(inputFile, outputDir,"apriori_map.aprioriMapperk","apriori_map.aprioriReducerk",conf);
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
		     
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
}
}
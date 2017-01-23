package apriori_map;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AssociationRules {
	public static double confidence;
	public static ArrayList<Map<String, Integer>> allFrequentItemsets = new ArrayList<Map<String, Integer>>();
	public static ArrayList<String[]> associationRules = new ArrayList<String[]>(); //Format: (String[0] = A, String[1] = B, String[2] = C) => A -> B, confidence C
	
	 /*
	   * Accepts: Necessary variables to generate association rules
	   * Returns: None
	   * Purpose: Constructor to initialize Association Rules class
	   */
	public AssociationRules(ArrayList<Map<String, Integer>> data, double confidenceTreshold) {
		confidence = confidenceTreshold;
		allFrequentItemsets = data;
	}
	
	 /*
	   * Accepts: None
	   * Returns: None
	   * Purpose: Displays the results of the association rules in the console
	   */
	public void displayAssociationRules() {
		findAssociationRules();
		System.out.println("\n==== Association Rules ====\n");
		for(int i=0; i<associationRules.size(); i++) {
			String[] rule = associationRules.get(i);
			System.out.println(rule[0] + " -> " + rule[1] + "\t" + rule[2]);
		}
	}
	
	 /*
	   * Accepts: Filename String
	   * Returns: None, but outputs data to file
	   * Purpose: Writes all association rules to the specified file
	   */
	public void writeAssociationRulesToFile(String filename) { 
		findAssociationRules();
		    try { 
		      BufferedWriter bWriter = new BufferedWriter(new FileWriter(filename)); 
				for(int i=0; i<associationRules.size(); i++) {
					String[] rule = associationRules.get(i);
					bWriter.write(rule[0] + " -> " + rule[1] + "\t" + rule[2] + "\n");
				}
		    } 
		    catch (Exception e) { 
		      System.out.println("Error reading input file."); 
		    } 
	} 
	
	 /*
	   * Accepts: None
	   * Returns: None
	   * Purpose: Manages the generation of association rules based on the data
	   */
	private static void findAssociationRules() {
		try {
			if(allFrequentItemsets.size() >= 2)
				findRulesForPairs();
			if(allFrequentItemsets.size() >= 3)
				findRulesForTriples();
			if(allFrequentItemsets.size() >= 4)
				findRulesForQuadruples();
		}
		catch (NullPointerException e) {}
	}
	
	 /*
	   * Accepts: An item set and a basket
	   * Returns: Returns items in the basket that are not in the item set
	   * Purpose: It finds the right side of the association rule
	   */
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
	
		remainingItems = Apriori.combineStrings(remainingItems.split(",")); // Sort string in ascending order
		
		return remainingItems;
	}
	
	/*
	 * Accepts: An item set and an integer size
	 * Returns: An list of all combinations of potential association rules, each combination is a 2D Strings
	 * Purpose: Generates all combinations of associations rules for a certain size which represents the length of the left side of the association rule
	 */
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

				if(comb.split(",").length == size) { //An additional check to make sure the item set has the correct number of items
					String associatedItems = getAssociation(comb, string); //Get right hand side values (comb is the left hand side of association rule)
					String[] association = new String[2];
					association[0] = comb;
					association[1] = associatedItems;
					assRules.add(association);
				}
			}
		}
		return assRules;
	}
	
	 /*
	   * Accepts: None, but reads data from allFrequentItemsets
	   * Returns: None, but adds association rules for frequent pairs to associationRules
	   * Purpose: Parses through the frequent pairs and finds all association rules that meet the user specified confidence
	   */
	private static void findRulesForPairs() {
		// Create data structure to store frequent items and frequent pairs
		Map itemset = new HashMap<String, Integer>();
		Map freqItems = new HashMap<String, Integer>();
		itemset = allFrequentItemsets.get(1);
		freqItems = allFrequentItemsets.get(0);
		
	    Iterator it = itemset.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        String[] items = pair.getKey().toString().split(",");
	        
	        int supportAandB = pair.getValue().hashCode(); //getValue.hashcode <-- converts value from Map.Entry to integer
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
	    }	
	}
	
	 /*
	   * Accepts: Two strings and a double (an association rule and its confidence)
	   * Returns: None
	   * Purpose: Converts the association rule to the required format and adds it to the data structure associationRules (which holds all the association rules)
	   */
	private static void addRule(String a, String b, double c) {
		String[] rule = new String[3];
    	rule[0] = a;
    	rule[1] = b;
    	rule[2] = Double.toString(c);
    	associationRules.add(rule);	 
	}
	
	 /*
	   * Accepts: None, but reads data from allFrequentItemsets
	   * Returns: None, but adds association rules for frequent triples to associationRules
	   * Purpose: Parses through the frequent triples and finds all association rules that meet the user specified confidence
	   */
	private static void findRulesForTriples() {
		//Create data structures to store frequent item sets of different values of k (pairs, triples)
		Map itemset = new HashMap<String, Integer>();
		Map freqPairs = new HashMap<String, Integer>();
		Map freqItems = new HashMap<String, Integer>();
		itemset = allFrequentItemsets.get(2);
		freqPairs = allFrequentItemsets.get(1);		
		freqItems = allFrequentItemsets.get(0);
		
		//Create an iterator to parse through the frequent triples
		Iterator it = itemset.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry line = (Map.Entry)it.next();
	        String triple = line.getKey().toString();
	        ArrayList<String[]> potentialRules = getCombinations(triple, 2); //Get combinations of size 2 (Two Items -> One Item)

	        for(int i=0; i<potentialRules.size(); i++) {
	        	// [Two Items -> One Item]
	        	String a = potentialRules.get(i)[0];
	        	String b = potentialRules.get(i)[1];
	        	int supportAandB = line.getValue().hashCode();
		        int supportA = freqPairs.get(a).hashCode();
		        double c = 1.0 * supportAandB/supportA;
		        if(c>=confidence)
		        	addRule(a, b, c);
		        
		        // Reverse [One Item -> Two Items]
		        a = potentialRules.get(i)[1];
	        	b = potentialRules.get(i)[0];
	        	supportAandB = line.getValue().hashCode();
		        supportA = freqItems.get(a).hashCode();
		        c = 1.0 * supportAandB/supportA;
		        if(c>=confidence)
		        	addRule(a, b, c);
	        }
	    }
	}
	
	/*
	   * Accepts: None, but reads data from allFrequentItemsets
	   * Returns: None, but adds association rules for frequent quadruples to associationRules
	   * Purpose: Parses through the frequent quadruples and finds all association rules that meet the user specified confidence
	   */
	private static void findRulesForQuadruples() {
		//Create data structures to store frequent item sets of different values of k (pairs, triples, quadruples)
		Map itemset = new HashMap<String, Integer>();
		Map freqTriples = new HashMap<String, Integer>();
		Map freqPairs = new HashMap<String, Integer>();
		Map freqItems = new HashMap<String, Integer>();
		itemset = allFrequentItemsets.get(3);
		freqTriples = allFrequentItemsets.get(2);
		freqPairs = allFrequentItemsets.get(1);		
		freqItems = allFrequentItemsets.get(0);
		
		//Create an iterator to parse through the frequent quadruples
		Iterator it = itemset.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry line = (Map.Entry)it.next();
	        String quads = line.getKey().toString();
	        
	        ArrayList<String[]> potentialRules = getCombinations(quads, 3); //Get combinations of size 3 (Three Items -> One Item)
	        for(int i=0; i<potentialRules.size(); i++) {
	        	// [Three Items -> One Item]
	        	String a = potentialRules.get(i)[0];
	        	String b = potentialRules.get(i)[1];
	        	int supportAandB = line.getValue().hashCode();
		        int supportA = freqTriples.get(a).hashCode();
		        double c = 1.0 * supportAandB/supportA;
		        if(c>=confidence)
		        	addRule(a, b, c);
		        
		        // Reverse [One Item -> Three Items]
		        a = potentialRules.get(i)[1];
	        	b = potentialRules.get(i)[0];
	        	supportAandB = line.getValue().hashCode();
		        supportA = freqItems.get(a).hashCode();
		        c = 1.0 * supportAandB/supportA;
		        if(c>=confidence)
		        	addRule(a, b, c);
	        }
	        
	        potentialRules = getCombinations(quads, 2); //Get combinations of size 2 (Two Items -> Two Item)
	        for(int i=0; i<potentialRules.size(); i++) {
	        	// [Two Items -> Two Item]
	        	String a = potentialRules.get(i)[0];
	        	String b = potentialRules.get(i)[1];
	        	int supportAandB = line.getValue().hashCode();
		        int supportA = freqPairs.get(a).hashCode();
		        double c = 1.0 * supportAandB/supportA;
		        if(c>=confidence)
		        	addRule(a, b, c);
	        }
	    }
	}
}

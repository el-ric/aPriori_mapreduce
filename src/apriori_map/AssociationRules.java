package apriori_map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AssociationRules {
	public static double confidence;
	public static ArrayList<Map<String, Integer>> allFrequentItemsets = new ArrayList<Map<String, Integer>>();
	public static ArrayList<String[]> associationRules = new ArrayList<String[]>(); //Format: (String[0] = A, String[1] = B, String[2] = C) => A -> B, confidence C

	public AssociationRules(ArrayList<Map<String, Integer>> data, double confidenceTreshold) {
		confidence = confidenceTreshold;
		allFrequentItemsets = data;
	}
	
	public void displayAssociationRules() {
		findAssociationRules();
		System.out.println("\n==== Association Rules ====\n");
		for(int i=0; i<associationRules.size(); i++) {
			String[] rule = associationRules.get(i);
			System.out.println(rule[0] + " -> " + rule[1] + "\t" + rule[2]);
		}
	}
	
	private static void findAssociationRules() {
		try {
			if(allFrequentItemsets.size() >= 2)
				findRulesForPairs();
			if(allFrequentItemsets.size() >= 3)
				findRulesForTriples();
			if(allFrequentItemsets.size() >= 4)
				findRulesForQuadruples();
		}
		catch (NullPointerException e) {
			// Ignore null pointer exceptions
		}
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
	
		remainingItems = Apriori.combineStrings(remainingItems.split(",")); // Sort string in ascending order
		
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

				if(comb.split(",").length == size) {
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
		// Create Data structures for data
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
		//Create Data structures for Data
		Map itemset = new HashMap<String, Integer>();
		Map freqPairs = new HashMap<String, Integer>();
		Map freqItems = new HashMap<String, Integer>();
		itemset = allFrequentItemsets.get(2);
		freqPairs = allFrequentItemsets.get(1);		
		freqItems = allFrequentItemsets.get(0);
		
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
	
	private static void findRulesForQuadruples() {
		Map itemset = new HashMap<String, Integer>();
		Map freqTriples = new HashMap<String, Integer>();
		Map freqPairs = new HashMap<String, Integer>();
		Map freqItems = new HashMap<String, Integer>();
		itemset = allFrequentItemsets.get(3);
		freqTriples = allFrequentItemsets.get(2);
		freqPairs = allFrequentItemsets.get(1);		
		freqItems = allFrequentItemsets.get(0);
		
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

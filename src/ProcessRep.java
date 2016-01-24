import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ProcessRep {
	
	// This static variable is only used for creating the file of requests
	private static int pagesOccupiedByProcess = 10;
	
	private static String fileForRequests = "pageRequests";

	int numberOfPagesOccupiedByProcess;
	// Members for providing stats of the page replacement algorithms
	int numberOfPageFaults;
	int minNumberOfFrames;
	int maxNumberOfFrames;
	int numberOfTimesLessThan10;
	
	Map<Integer, Boolean> memoryRep;
	List<Integer> requests;


	public ProcessRep(String fileName) throws NumberFormatException, IOException {
		this.memoryRep = new HashMap<Integer, Boolean>();
		this.requests = new ArrayList<Integer>();
		
		// Fill the requests list from the file
		File reqFile = new File(fileName);
		@SuppressWarnings("resource")
		BufferedReader buffReader = new 
				BufferedReader(new FileReader(reqFile));
		this.numberOfPageFaults = 0;
		this.minNumberOfFrames = 1;
		this.maxNumberOfFrames = 0;
		this.numberOfTimesLessThan10 = 0;

		this.numberOfPagesOccupiedByProcess = Integer.
				parseInt(buffReader.readLine());
		String line = "";
		while((line = buffReader.readLine()) != null) {
			this.requests.add(Integer.parseInt(line));
		}
	}

	public int getNumberOfPageFaults() {
		return this.numberOfPageFaults;
	}
	
	public int getMinNumberOfFrames() {
		return this.minNumberOfFrames;
	}
	
	public int getMaxNumberOfFrames() {
		return this.maxNumberOfFrames;
	}
	
	public int getNumberOfTimesLessThan10() {
		return this.numberOfTimesLessThan10;
	}
	public void servePageRequestsWithPFF(int threshold) {
		int pageFaultPosition = -1;
		// Serve all requests
		for(int index = 0; index < requests.size(); index++) {
			int pageNumber = requests.get(index);
			if(memoryRep.containsKey(pageNumber)) {
				// Page already in memory
				if(!memoryRep.get(pageNumber)) {
					memoryRep.put(pageNumber, true);
				}
			} else {
				// Page fault
				this.numberOfPageFaults++;
				if(pageFaultPosition >= 0) {
					// Not the first page fault
					int virtualTimeSinceLastPageFault = (index - 
							pageFaultPosition);
					pageFaultPosition = (index);
					
					if(virtualTimeSinceLastPageFault >= threshold) {
						// Have to reduce working set since memory allocated
						// may be too much
						List<Integer> pagesToRemove = new ArrayList<Integer>();
						for(Integer i : memoryRep.keySet()) {
							if(!memoryRep.get(i)) {
								pagesToRemove.add(i);
							} else {
								// Reset the use bit for everything present
								memoryRep.put(i, false);
							}
						}
						for(Integer i : pagesToRemove) {
							memoryRep.remove(i);
						}
						
						// Add the one which caused page fault
						memoryRep.put(pageNumber, true);
					} else {
						// Have to increase working set since too low memory
						// allocated to process
						memoryRep.put(pageNumber, true);
						
					}
				} else {
					// First Page fault while serving process
					pageFaultPosition = 0;
					memoryRep.put(pageNumber, true);
				}
				
			}
			
			if(memoryRep.size() < 10) {
				this.numberOfTimesLessThan10++;
			}
			this.minNumberOfFrames = Math.min(this.minNumberOfFrames, memoryRep.size());
			this.maxNumberOfFrames = Math.max(this.maxNumberOfFrames, memoryRep.size());
		}
	}

	public void severPageRequestsWithVSWS(int minSamplingTime, int maxSamplingTime,
			int pageFaultsAllowed) {
		int virtualTimeWithinInterval = 0;
		int pageFaultsWithinInterval = 0;
		
		// Serve the requests
		for(int index = 0; index < requests.size(); index++) {
			int pageNumber = requests.get(index);
			
			virtualTimeWithinInterval++;
			boolean isPageFault = false;
			
			// PROCEDURE FOR VSWS...!
			// First increment the virtualTime
			// Then see if page already in memory
			// If yes, then too continue to check if sampling interval reached
			// If Sampling interval reached and pageFault then first empty set and then insert
			// If not pageFault then just shrink set
			if(memoryRep.containsKey(pageNumber)) {
				// Already in memory
				if(!memoryRep.get(pageNumber)) {
					memoryRep.put(pageNumber, true);
				} else {
					// Do nothing if page already has a use bit set
				}
			} else {
				// Its a pageFault
				isPageFault = true;
			}
			
			// Now check if virtual time overshadowed maxSamplingTime
			if(virtualTimeWithinInterval >= maxSamplingTime) {
				// Completed maxSamplingTime virtual time
				shrinkWorkingSet(memoryRep);
				if(isPageFault) {
					memoryRep.put(pageNumber, true);
					//pageFaultsWithinInterval++;
					this.numberOfPageFaults++;
				}
				virtualTimeWithinInterval = 0;
				pageFaultsWithinInterval = 0;
			} else {
				// Virtual Time still within sampling interval
				if(pageFaultsWithinInterval >= pageFaultsAllowed) {
					// Page Faults within interval exceeded
					if(virtualTimeWithinInterval >= minSamplingTime) {
						// Now we can scan and remove pages not referenced
						shrinkWorkingSet(memoryRep);
						//return;
						if(isPageFault) {
							memoryRep.put(pageNumber, true);
							// No need of increasing the below counter since
							// this has to be set to 0
							//pageFaultsWithinInterval++;
							this.numberOfPageFaults++;
						}
						virtualTimeWithinInterval = 0;
						pageFaultsWithinInterval = 0;
					} else {
						// Wait till this time reaches min sampling interval
						if(isPageFault) {
							memoryRep.put(pageNumber, true);
							pageFaultsWithinInterval++;
							this.numberOfPageFaults++;
						}
					}
				} else {
					// Everything normal, just increase working set
					if(isPageFault) {
						memoryRep.put(pageNumber, true);
						pageFaultsWithinInterval++;
						this.numberOfPageFaults++;
					}
				}
			}
			if(memoryRep.size() < 10) {
				this.numberOfTimesLessThan10++;
			}
			this.minNumberOfFrames = Math.min(this.minNumberOfFrames, memoryRep.size());
			this.maxNumberOfFrames = Math.max(this.maxNumberOfFrames, memoryRep.size());
		}
	}
	
	/**
	 * 
	 * @param memoryRep
	 * @param pageNumber
	 * @return true if new page to be added is already there.
	 * Shrinks the working set. After shrinking inserts the new page if not already present
	 */
	private void shrinkWorkingSet(Map<Integer, Boolean> memoryRep) {
		List<Integer> pagesToRemove = new ArrayList<Integer>();
		for(Integer i : memoryRep.keySet()) {
			if(!memoryRep.get(i)) {
				pagesToRemove.add(i);
			} else {
				// Reset the use bit for everything present
				memoryRep.put(i, false);
			}
		}
		for(Integer i : pagesToRemove) {
			memoryRep.remove(i);
		}
		
	}

	/**
	 * Uses the random function which generates random page requests
	 * @throws IOException 
	 */
	public static void  generateFileWithRandomPageRequests(String 
			pageReqFileName) throws IOException {
		StringBuffer data = new StringBuffer();
		data.append(pagesOccupiedByProcess);
		data.append("\n");
		
		Random randomGen = new Random();
		for(int i=0; i<10000; i++) {
			int randomInt = randomGen.nextInt(pagesOccupiedByProcess);
			data.append(randomInt);
			data.append("\n");
		}
		
		FileWriter writer = new FileWriter(new File(fileForRequests));
		BufferedWriter buffWriter = new BufferedWriter(writer);
		buffWriter.write(data.toString());
		buffWriter.close();
		
	}

	/**
	 * 
	 * @param args[0] is the fileName of the data
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try {
			// IMPACT of F(threshold) on number of page faults:
			// As the value of F increases, number of pageFaults decreases
			ProcessRep pff = new ProcessRep(args[0]);
			System.out.println("For Page Fault Frequency Algorithm......");
			pff.servePageRequestsWithPFF(1);
			
			System.out.println("Total Page faults = " + pff.getNumberOfPageFaults());
			System.out.println("Min number of frames for the process in memory"
					+ "= " + pff.getMinNumberOfFrames());
			System.out.println("Number of times that pages in memory for process"
					+ "is less than 10 = " + pff.getNumberOfTimesLessThan10());
			System.out.println("Max number of frames for the process in memory"
					+ "= " + pff.getMaxNumberOfFrames());
			
			
			// IMPACT of M(minSamplingInterval), L(maxSamplingInterval), Q(maxPageFaultsInInterval)
			// Number of page faults depend on all M, L and Q.
			// If we fix Q and increase the difference between L and M then pageFaults reduces
			// But if we fix M and L and vary Q then numberOfPageFaults first increase and then decrease
			ProcessRep vsws = new ProcessRep(args[0]);
			System.out.println("\nFor VSWS Algorithm....");
			vsws.severPageRequestsWithVSWS(5, 10, 5);
			
			
			System.out.println("Total Page faults = " + vsws.getNumberOfPageFaults());
			System.out.println("Min number of frames for the process in memory"
					+ " = " + vsws.getMinNumberOfFrames());
			System.out.println("Number of times that pages in memory for process"
					+ "is less than 10 = " + vsws.getNumberOfTimesLessThan10());
			System.out.println("Max number of frames for the process in memory"
					+ " = " + vsws.getMaxNumberOfFrames());
		} catch(IOException e) {
			System.out.println("IO error Occured: " + e.getMessage());
		}
	}
}

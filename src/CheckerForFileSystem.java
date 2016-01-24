import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Deepesh
 * Class has all the utility functions from reading a file to string processing
 */
class UtilForFileSystem {
	public static String readFileIntoString(String pathName) throws IOException { 
		BufferedReader buffReader = null;
		StringBuffer fileData = null;
		try {
			File file = new File(pathName);
			buffReader = new BufferedReader(new FileReader(file));
			
			String line = null;
			fileData = new StringBuffer();
			while((line = buffReader.readLine())!= null) {
				fileData.append(line);
				fileData.append("\n");
			}
		} catch (IOException e) {
			throw e;
		} finally {
			buffReader.close();
		}
		return fileData.toString();
	}
	public static Map<String, Object> deSerializeToMap(String blockData) { 
		Map<String, Object> superMap = new HashMap<String, Object>();
		
		// Replacing all {,}, space characters
		blockData = blockData.substring(1, blockData.length()-2);
		blockData = blockData.replaceAll("[\\s]", "").toLowerCase();
		
		String subEntries = "";
		int dictIndex = blockData.indexOf("filename_to_inode_dict");
		if(dictIndex != -1) {
			int startIndex = blockData.indexOf("{")+1;
			int endIndex = blockData.indexOf("}");
			subEntries = blockData.substring(startIndex, endIndex);
			
			dictIndex -=1;
			superMap.put("filename_to_inode_dict", subEntries);
			
		} else {
			dictIndex = blockData.length();
		}
		String[] parts = blockData.substring(0, dictIndex).split(",");
		
		for(String part : parts) {
			String[] keyValueTuple = part.split(":");
			if(keyValueTuple.length != 2) {
				continue;
			}		
			
			if(keyValueTuple[0].contains("time")) {
				superMap.put(keyValueTuple[0], new 
						Long(Long.parseLong(keyValueTuple[1])));
			} else {
				superMap.put(keyValueTuple[0], new 
						Integer(Integer.parseInt(keyValueTuple[1])));
			}
		}
		return superMap;
	}
	public static String serializeFromMap(Map<String, Object> blockMap) {
		String serializedString = "{";
		String inodeMappingString = "";
		if(blockMap.containsKey("filename_to_inode_dict")) {
			inodeMappingString += ",filename_to_inode_dict:";
			inodeMappingString += "{";
			inodeMappingString += (String)blockMap.get("filename_to_inode_dict");
			inodeMappingString += "}";
		}
		
		for(String s : blockMap.keySet()) {
			if(!s.equals("filename_to_inode_dict")) {
				serializedString += s;
				serializedString += ":";
				serializedString += blockMap.get(s);
				serializedString += ",";
			}
		}
		serializedString = serializedString.substring(0, serializedString.length()-1);
		serializedString +=inodeMappingString;
		serializedString += "}";
		return serializedString;
	}
	public static void writeToFile(File fileToWrite, String dataToWrite) throws Exception {
		if(!fileToWrite.exists())
			throw new Exception("File to be written does not exist");
		
		FileWriter writer = new FileWriter(fileToWrite);
		BufferedWriter buffWriter = new BufferedWriter(writer);
		buffWriter.write(dataToWrite);
		buffWriter.close();
	}
	public static List<Integer> deserializeToArray(String data) {
		List<Integer> allBlkNumbers = new ArrayList<Integer>();
		// Replacing all {,}, space characters
		data = data.replaceAll("[\\s]", "").toLowerCase();
		
		String[] tempNumbsAsString = data.split(",");
		for(String s : tempNumbsAsString) {
			allBlkNumbers.add(Integer.parseInt(s));
		}
		return allBlkNumbers;
	}
	public static String serializeFromArray(List<Integer> blocks) { 
		// Sort the blocks first
		blocks.sort(new Comparator<Integer>() {
			@Override
			public int compare(Integer arg0, Integer arg1) {
				return arg0-arg1;
			}
		});
		
		String serializedString = "";
		for(Integer i : blocks) {
			serializedString += i;
			serializedString += ",";
		}
		serializedString = serializedString.substring(0, serializedString.length()-1);
		return serializedString;
	}
}

/**
 * 
 * @author Deepesh
 * Base class for SuperBlock, FreelistBlock, DirectoryINodeBlock, FileINodeBlock 
 */
abstract class Block {
	public static final String BLOCK_NAME = "fusedata";
	public static final int MAX_BLOCK_SIZE = 4096;
	public static final int MAX_POINTERS_IN_BLOCK = 400;

	private int blockNum;
	private int parentBlockNum;
	protected Map<String, Object> deSerializedBlock;
	
	Block(int blockNum, int parentBlockNum) throws IOException {
		this.blockNum = blockNum;
		this.parentBlockNum = parentBlockNum;
		this.deSerializedBlock = deSerializeThisBlock();
	}
	protected void checkAllTimesInBlock() {
		Long currentTime = System.currentTimeMillis()/1000L;
		for(String key : deSerializedBlock.keySet()) {
			if(key.contains("time")) {
				if((Long)deSerializedBlock.get(key) >= currentTime) {
					CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Time is "
							+ "not in past for key " + key +
							" in block " + BLOCK_NAME + "." + this.blockNum);
					
					// Check block by putting everything as current time
					deSerializedBlock.put(key, currentTime);
				}
			}
		}
	}
	protected int getBlockNumber() {
		return blockNum;
	}
	protected int getParentBlockNumber() {
		return parentBlockNum;
	}
	protected Map<String, Object> deSerializeThisBlock() throws IOException {
		String blockDataAsString = UtilForFileSystem.readFileIntoString(constructPath());
		return UtilForFileSystem.deSerializeToMap(blockDataAsString);
	}
	public void check() throws Exception {
		performCheckForBlock();
		prepareNextCheck();
		
		// Write only if it has errors
		if(!CheckerForFileSystem.FILE_CHECKER_ERRORS.isEmpty())
			writeBlockToFile();
	}
	protected void writeBlockToFile() throws Exception {
		String correctedString = UtilForFileSystem.serializeFromMap(deSerializedBlock);
		UtilForFileSystem.writeToFile(new File(constructPath()),
				correctedString);
	}
	protected String constructPath(Integer... blockNumber) {
		return CheckerForFileSystem.FILE_SYSTEM_PATH + "/" + BLOCK_NAME +
				"." + ((blockNumber.length > 0) ? blockNumber[0] : this.blockNum);
	}
	protected abstract void performCheckForBlock() throws IOException, Exception;
	protected abstract void prepareNextCheck() throws IOException, Exception;
}

class SuperBlock extends Block{

	SuperBlock() throws IOException {
		// Super block number is always 0 and parent is not applicable
		super(0, -1);
	}
	@Override
	protected void performCheckForBlock() throws Exception {
		// Check for Device Id
		if((Integer)this.deSerializedBlock.get("devid") != CheckerForFileSystem.DEVICE_ID) {
			CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Device Id of the system"
					+ "is not right. It should be " + CheckerForFileSystem.DEVICE_ID);
			throw new Exception("This File Checker is for deviceId " + CheckerForFileSystem.
					DEVICE_ID + " not for " + (Integer)this.deSerializedBlock.get("devid"));
		}

		// Check if all times are in past
		this.checkAllTimesInBlock();
	}
	@Override
	protected void prepareNextCheck() throws Exception {
		int rootBlockNum = (Integer)this.
				deSerializedBlock.get("root");
		DirectoryINodeBlock root = new DirectoryINodeBlock(rootBlockNum, rootBlockNum);
		root.check();
		
		FreeBlockList freeBlockList = new FreeBlockList((Integer)this.
				deSerializedBlock.get("maxblocks"), (Integer)this.
				deSerializedBlock.get("freestart"), (Integer)this.
				deSerializedBlock.get("freeend"));
		freeBlockList.check();
	}
}

class DirectoryINodeBlock extends Block {

	class Triplet {
		boolean isFile;
		String name;
		int blockNumber;
		
		String serializeTriplet() {
				return ((isFile)?'f':'d') + ":" + name + ":" + blockNumber;
		}
	}

	DirectoryINodeBlock(int blockNum, int parentBlockNum) throws IOException {
		super(blockNum, parentBlockNum);
	}
	@Override
	protected void performCheckForBlock() throws Exception {
		// Check if all the times are in past
		checkAllTimesInBlock();
		
		List<Triplet> children = parseFileToInodeDict((String)this.deSerializedBlock.
				get("filename_to_inode_dict"));
		
		// Check if both . and .. are present and check for all files
		// Also check if linkCount matches
		int linkCount = (Integer)this.deSerializedBlock.get("linkcount");
		if(linkCount != children.size()) {
			CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Link count does"
					+ "not match for DirectoryINode Block# " + this.getBlockNumber());
			
			// Correct Block
			this.deSerializedBlock.put("linkcount", children.size());
		}

		boolean checkForSelf = false;
		boolean checkForParent = false;
		List<Triplet> tripletsToRemove = new ArrayList<Triplet>();
		for(Triplet t : children) {
			if(t.isFile) {
				FileINodeBlock file = new FileINodeBlock(t.blockNumber,
						this.getBlockNumber());
				file.check();
			} else if(t.name.equals(".")) {
				if(checkForSelf) {
					tripletsToRemove.add(t);
					continue;
				}

				checkForSelf = true;
				if(t.blockNumber != this.getBlockNumber()) {
					CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Wrong block "
							+ "number in filename to inode dictionary"
							+ " for . in block# " + this.getBlockNumber());
					
					// Correct Block
					t.blockNumber = this.getBlockNumber();
				}
			} else if(t.name.equals("..")) {
				if(checkForParent) {
					tripletsToRemove.add(t);
					continue;
				}

				checkForParent = true;
				if(t.blockNumber != this.getParentBlockNumber()) {
					CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Wrong block "
							+ "number in filename to inode dictionary"
							+ " for .. in block# " + this.getBlockNumber());
					
					// Check Block
					t.blockNumber = this.getParentBlockNumber();
				}
			} else {
				DirectoryINodeBlock dir = new DirectoryINodeBlock(t.blockNumber,
						this.getBlockNumber());
				dir.check();
			}
		}
		
		children.removeAll(tripletsToRemove);
		if(!(checkForSelf & checkForParent)) {
			CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Both . and .. not present "
					+ "in the dictionary of block# " + this.getBlockNumber());
			
			// Correct file here
			if(!checkForSelf) {
				Triplet temp = new Triplet();
				temp.blockNumber = this.getBlockNumber();
				temp.isFile = false;
				temp.name = ".";
				children.add(temp);
			} else if(!checkForParent) {
				Triplet temp = new Triplet();
				temp.blockNumber = this.getParentBlockNumber();
				temp.isFile = false;
				temp.name = "..";
				children.add(temp);
			}
		}
		
		// Correct Block
		this.deSerializedBlock.put("filename_to_inode_dict", 
				convertChildrenTripletsToString(children));	
	}
	private String convertChildrenTripletsToString(List<Triplet> children) {
		String inodeString = "";
		for(Triplet child : children) {
			inodeString += child.serializeTriplet();
			inodeString += ",";
		}
		return inodeString.substring(0, inodeString.length()-1);
	}
	private List<Triplet> parseFileToInodeDict(String inodeInf) {
		String[] partsOfInodeDict = inodeInf.split(",");
		List<Triplet> triples = new ArrayList<Triplet>();
		for(String s : partsOfInodeDict) {
			Triplet t = new Triplet();
			String[] parts = s.split(":");
			
			if(parts[0].equals("f"))
				t.isFile = true;
			else
				t.isFile = false;
			
			t.name = parts[1];
			t.blockNumber = Integer.parseInt(parts[2]);
			triples.add(t);
		}
		return triples;
	}
	@Override
	protected void prepareNextCheck() {
		// Preparation for next checks have already been done in checkMethod
	}
}

class FileINodeBlock extends Block {

	boolean isIndirect;
	int locationBlock;

	FileINodeBlock(int blockNum, int parentBlockNum) throws IOException {
		super(blockNum, parentBlockNum);
		isIndirect = ((Integer)this.deSerializedBlock.get("indirect") == 1) ? true : false;
		locationBlock = ((Integer)this.deSerializedBlock.get("location"));
	}
	@Override
	protected void performCheckForBlock() throws Exception {
		
		// Check if all times are in past
		checkAllTimesInBlock();
		
		// Check if the size is proper
		int size = ((Integer)this.deSerializedBlock.get("size"));
		
		if(!this.isIndirect) {
			if(size > Block.MAX_BLOCK_SIZE) {
				CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Invalid Size in the "
						+ "file INode for block# " + this.getBlockNumber());
				
				// Correct file here
				this.deSerializedBlock.put("size", Block.MAX_BLOCK_SIZE);
			}
		} else {
			// Check if location pointer points to an array => indirect=1
			List<Integer> locationsFileHasBeenStored = 
					getLocationsPointedByLocationPtr();
			if(locationsFileHasBeenStored == null) {
				CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Location pointer "
						+ "does not point to an array even after indirect for "
						+ "the file = 1 for block# " + this.getBlockNumber());
			}

			if(locationsFileHasBeenStored.size() > 1 && size < Block.MAX_BLOCK_SIZE) {
				CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Invalid Size in the "
						+ "file INode for block# " + this.getBlockNumber());
			} else if(size > Block.MAX_BLOCK_SIZE*locationsFileHasBeenStored.size()) {
				CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Invalid Size in the "
						+ "file INode for block# " + this.getBlockNumber());
			}
		}
	}
	private List<Integer> getLocationsPointedByLocationPtr() throws IOException {
		String locationData = UtilForFileSystem.readFileIntoString(constructPath(
				this.locationBlock));

		locationData = locationData.replaceAll("[\\s]", "").toLowerCase();
		List<Integer> locations = new ArrayList<Integer>();

		String[] parts = locationData.split(",");
		for(String s : parts) {
			try {
				locations.add(Integer.parseInt(s));
			} catch(NumberFormatException e) {
				// We return null if its not an array since if it was a proper
				// array in csv format then we wouldn't have entered here
				return null;
			}
		}
		return locations;
	}
	@Override
	protected void prepareNextCheck() {}
}

/**
 * 
 * @author Deepesh
 * 
 */
class FreeBlockListBlock extends Block {

	private int startFreeBlock;
	private int endFreeBlock;
	private List<Integer> allBlockNumbersInBlock;
	
	FreeBlockListBlock(int blockNum, int parentBlockNum, int startFreeBlock,
			int endFreeBlock) throws IOException {
		// No meaning of parent for this block
		super(blockNum, parentBlockNum);
		this.startFreeBlock = startFreeBlock;
		this.endFreeBlock = endFreeBlock;
	}
	protected int getFirstBlock() {
		return startFreeBlock;
	}
	protected int getLastBlock() {
		return endFreeBlock;
	}
	@Override
	public void writeBlockToFile() throws Exception {
		String correctedString = UtilForFileSystem.
				serializeFromArray(allBlockNumbersInBlock);
		UtilForFileSystem.writeToFile(new File(constructPath()), correctedString);
		
	}
	@Override
	public Map<String, Object> deSerializeThisBlock() throws IOException {
		String blockDataAsString = UtilForFileSystem.readFileIntoString(constructPath());
		this.allBlockNumbersInBlock = UtilForFileSystem.
				deserializeToArray(blockDataAsString);
		
		// Sort the block numbers so that its easy to check
		this.allBlockNumbersInBlock.sort(new Comparator<Integer>() {
			@Override
			public int compare(Integer num1, Integer num2) {
				return num1-num2;
			}
		});

		// In this particular block we do not need a map.
		return null;
	}
	@Override
	protected void performCheckForBlock() {

		// Check if all the blocks from startFreeBlock to end free block is there
		List<Integer> blocksMissing = new ArrayList<Integer>();
		int blockIterator = startFreeBlock;
		while(blockIterator < endFreeBlock) {
			for(int j=0; j<allBlockNumbersInBlock.size();j++) {
				if(allBlockNumbersInBlock.get(j) == blockIterator) {
					blockIterator++;
				} else {
					CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Free blocks missing "
							+ "from " + blockIterator
							+ " to " + (allBlockNumbersInBlock.get(j+1)-1) + " in free "
									+ "list block fusedata." + this.getBlockNumber());
					
					// Add Missing blocks
					for(int i=blockIterator; i<allBlockNumbersInBlock.get(j+1)-1; i++) {
						blocksMissing.add(i);
					}
					
					blockIterator = allBlockNumbersInBlock.get(j+1);
					
				}
			}
		}

		allBlockNumbersInBlock.addAll(blocksMissing);
	}
	@Override
	protected void prepareNextCheck() {}
}

/**
 * 
 * @author Deepesh
 * Container for all blocks containing the free block lists
 */
class FreeBlockList {
	private int maxBlocksInFileSystem;
	private int startBlockNumber;
	private int endBlockNumber;
	List<FreeBlockListBlock> freeBlockList;
	
	public FreeBlockList(int maxBlocks, int startBlock, int endBlock) throws IOException {
		this.maxBlocksInFileSystem = maxBlocks;
		this.startBlockNumber = startBlock;
		this.endBlockNumber = endBlock;
		
		// Will be filled while checking since before filling some conditions need to be checked
		freeBlockList = new ArrayList<FreeBlockListBlock>();
	}
	private void fillAllFreeBlocks(List<FreeBlockListBlock> freeList) throws IOException {
		int startIndexForBlock = CheckerForFileSystem.NUMBER_OF_BLOCKS_OCCUPIED;
		int endIndexForBlock = Block.MAX_POINTERS_IN_BLOCK-1;
		for(int i=startBlockNumber, j=0; i<=endBlockNumber; i++,j++) {
			FreeBlockListBlock blk = new FreeBlockListBlock(i, -1, startIndexForBlock,
					endIndexForBlock);
			freeList.add(blk);
			
			startIndexForBlock = endIndexForBlock+1;
			
			// We need this because total blocks can be less than total
			// pointers which we can store in all list blocks
			int tempEndIndex = endIndexForBlock + Block.MAX_POINTERS_IN_BLOCK;
			endIndexForBlock = (tempEndIndex > maxBlocksInFileSystem) ? 
				maxBlocksInFileSystem : tempEndIndex;
		}
	}
	public void check() throws Exception {
		int expectedFreeBlockListLength = maxBlocksInFileSystem/
				(Block.MAX_POINTERS_IN_BLOCK);
		if (expectedFreeBlockListLength > ((endBlockNumber-startBlockNumber)+1)) {
			CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Number of free block "
					+ "list blocks are not sufficient");
		}
		fillAllFreeBlocks(freeBlockList);

		if(!freeBlockList.isEmpty()) {
			for(FreeBlockListBlock freeListBlock : freeBlockList) {
				freeListBlock.check();
			}
		}
		
		// Also check that no files/directories are stored in free blocks
		// P.S - Probably not a good way to do this, but I am very lazy now
		// to make it better
		int firstFreeBlockNum = freeBlockList.get(0).getFirstBlock();
		if(firstFreeBlockNum < CheckerForFileSystem.NUMBER_OF_BLOCKS_OCCUPIED) {
			for(int temp = startBlockNumber; temp < CheckerForFileSystem.
					NUMBER_OF_BLOCKS_OCCUPIED; temp++) {
				String path = CheckerForFileSystem.FILE_SYSTEM_PATH + "/" + Block.BLOCK_NAME +
						"." + temp;
				File f = new File(path);
				if(f.getTotalSpace() > 0) {
					CheckerForFileSystem.FILE_CHECKER_ERRORS.add("Free blocks "
							+ "are not empty. Something wrong is here");
				}
			}
		}
	}
}

public class CheckerForFileSystem {

	public static final String FILE_SYSTEM_PATH = "C:/Users/Deepesh/Desktop/FS";
	public static List<String> FILE_CHECKER_ERRORS = new ArrayList<String>();
	public static final int DEVICE_ID = 20;
	public static final int NUMBER_OF_BLOCKS_OCCUPIED = getTotalNumberOfFilledBlocks();
	
	public void checkFileSystem() throws Exception {
		Block bossBlock = new SuperBlock();
		bossBlock.check();
		
		if(FILE_CHECKER_ERRORS.isEmpty()) {
			System.out.println("No errors in the file system.");
		} else {
			System.out.println("........See the list below for all the errors......\n");
			for(int i=0; i<FILE_CHECKER_ERRORS.size(); i++) {
				System.out.println(i+1 + ". " + FILE_CHECKER_ERRORS.get(i));
			}
		}
	}
	private static int getTotalNumberOfFilledBlocks() {
		File systemFolder = new File(FILE_SYSTEM_PATH);
		if(systemFolder.isDirectory()) {
			return systemFolder.listFiles().length;
		}
		return -1;
	}

	public static void main(String[] args) {
		CheckerForFileSystem csefsck = new CheckerForFileSystem();
		try {
			csefsck.checkFileSystem();
		} catch (Exception e) {
			System.out.println("Exception occured while checking File-System. Please"
					+ "see the trace below. Thanks\n");
			e.printStackTrace();
		}
	}
}
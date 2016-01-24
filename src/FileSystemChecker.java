import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

// Not the final draft. Tried this one first but was not able to correct all errors generically.
class FileSystemEntry {
	boolean isFile;
	int blockNumber;
	String name;
	FileSystemEntry parent;
	
	public FileSystemEntry(boolean isItAFile, int blockNum, FileSystemEntry par) {
		isFile = isItAFile;
		blockNumber = blockNum;
		name = "";
		parent = par;
	}
	
	public FileSystemEntry getParent() {
		return parent;
	}
	
	public boolean isFile() {
		return isFile;
	}
	
	public int getBlockNum() {
		return blockNumber;
	}
	
	public void setName(String dirName) {
		name = dirName;
	}
}

class Directory extends FileSystemEntry {
	List<FileSystemEntry> fileEntries;

	Directory(int rootNum, FileSystemEntry parent) {
		super(false, rootNum, parent);
		fileEntries = new ArrayList<FileSystemEntry>();
	}
	
	void addFileSystemEntry(FileSystemEntry f) {
		fileEntries.add(f);
	}
}

class FileEntry extends FileSystemEntry {
	boolean isRedirect;
	int locationPointerBlock;
	int size;

	public FileEntry(int blNum, boolean redirect, FileSystemEntry parent) {
		super(true, blNum, parent);
		isRedirect = redirect;
	}

}

class FileSystemTree {
	private static List<String> CHECKER_ERRORS_FOR_TREE = new ArrayList<String>();
	Directory root;
	
	FileSystemTree(Directory rootDirec) {
		root = rootDirec;
	}

	void parseSystemToBuildTree(File[] entriesInFileSystem) 
			throws IOException {
		Queue<FileSystemEntry> queue = new ArrayDeque<FileSystemEntry>();
		queue.add(root);
		
		while(!queue.isEmpty()) {
			FileSystemEntry firstItemInQueue = queue.remove();
			StringBuffer fileData = FileSystemUtil
					.readFileIntoString(entriesInFileSystem[firstItemInQueue.getBlockNum()]);
			Map<String, Object> fileMap = FileSystemUtil
					.convertStringToMap(fileData);
			FileSystemChecker.checkAllTimesInBlock(fileMap);
			if(!firstItemInQueue.isFile) {
				int linkCount = (Integer)fileMap.get("linkcount");
				String inodeDict = (String)fileMap.get("filename_to_inode_dict");
				if(linkCount != inodeDict.split(",").length) {
					CHECKER_ERRORS_FOR_TREE.add("Link Count dosent match");
				}
				List<FileSystemEntry> children = getChildrenAndCheckParent(fileMap, root);
				queue.addAll(children);
			} else {
				checkFileInode(firstItemInQueue, fileMap);
			}
		}
		
	}

	private void checkFileInode(FileSystemEntry firstItemInQueue, Map<String, Object> fileMap) throws IOException {
		if(firstItemInQueue.isFile()) {
			StringBuffer fileData = FileSystemUtil.readFile(FileSystemChecker.
					getSystemFolder().getAbsolutePath() + "/fusedata." + 
						(Integer)fileMap.get("location"));
			
			// Check if the data is an array
			int redirect = (Integer)fileMap.get("indirect");
			String[] parts = fileData.toString().split(",");
			boolean shouldWeCheckSize = true;
			if(redirect == 1) {
				try {
					for(String part : parts) {
						Integer.parseInt(part);					}
				} catch(NumberFormatException e) {
					shouldWeCheckSize = false;
					System.out.println(e.getLocalizedMessage());
					CHECKER_ERRORS_FOR_TREE.add("Location is not an array even after indirect = 1");
					CHECKER_ERRORS_FOR_TREE.add("Since location is not an array, size too is messed up");
				}
			}
			
			boolean sizeErrorIsThere = true;
			if(shouldWeCheckSize) {
				int size = (Integer)fileMap.get("size");
				if(redirect == 0 && size > FileSystemChecker.MAX_BLOCK_SIZE) {
					sizeErrorIsThere = false;
				} else if (redirect == 1 && size > FileSystemChecker.MAX_BLOCK_SIZE*parts.length) {
					sizeErrorIsThere = false;
				}
			}
			if(sizeErrorIsThere) {
				CHECKER_ERRORS_FOR_TREE.add("Size not Valid");
			}
		}
	}

	private List<FileSystemEntry> getChildrenAndCheckParent(Map<String, Object> fileMap, Directory parent) {
		List<FileSystemEntry> fileEntries = new ArrayList<FileSystemEntry>();
		String map = (String)fileMap.get("filename_to_inode_dict");
		String[] parts = map.split(",");

		boolean entryForCurrentNode = false;
		boolean entryForParentNode = false;

		for(String part : parts) {
			int startIndex = part.indexOf(":", 0);
			int endIndex = part.indexOf(":", startIndex+1);
			FileSystemEntry entry = null;
			if(part.startsWith("f")) {
				entry = new FileEntry(Integer.parseInt(part
						.substring(endIndex+1)), true, parent);
			} else {
				String direcName = part.substring(startIndex+1, endIndex);
				int blockNum = Integer.parseInt(part
						.substring(endIndex+1));

				if(direcName.equalsIgnoreCase(".")) {
					entryForCurrentNode = true;
					if(blockNum != parent.getBlockNum()) {
						CHECKER_ERRORS_FOR_TREE.add("Block Number of . "
								+ "and current file dosent match");
					}
				} else if(direcName.equalsIgnoreCase("..")) {
					entryForParentNode = true;
					FileSystemEntry parrentOfParent = parent.getParent();
					int blockNumToCheck;
					
					if(parrentOfParent == null) {
						blockNumToCheck = parent.getBlockNum();
					} else {
						blockNumToCheck = parrentOfParent.getBlockNum();
					}
					
					if(blockNum != blockNumToCheck) {
						CHECKER_ERRORS_FOR_TREE.add("Block Number of .. "
								+ "and current file dosent match");
					}

				} else {
					entry = new Directory(blockNum, parent);
				}
			}

			if(entry != null) {
				entry.setName(part.substring(startIndex+1, endIndex));
				fileEntries.add(entry);
				parent.addFileSystemEntry(entry);
			}
		}
		
		if(!parent.isFile && !(entryForCurrentNode & entryForParentNode)) {
			CHECKER_ERRORS_FOR_TREE.add("Both . and .. are not present");
		}

		return fileEntries;
	}

}

class FileSystemUtil {
		@SuppressWarnings("see if we can implement in a cleaner way")
		public static Map<String, Object> convertStringToMap(StringBuffer superBlockFile) {
			Map<String, Object> superMap = new HashMap<String, Object>();
			String fileData = superBlockFile.toString();
			
			// Replacing all {,}, space characters
			fileData = fileData.substring(1, fileData.length()-2);
			String parsedFileData = fileData.replaceAll("[\\s]", "").toLowerCase();
			
			String subEntries = "";
			int dictIndex = parsedFileData.indexOf("filename_to_inode_dict");
			if(dictIndex != -1) {
				int startIndex = parsedFileData.indexOf("{")+1;
				int endIndex = parsedFileData.indexOf("}");
				subEntries = parsedFileData.substring(startIndex, endIndex);
				dictIndex -=1;
				superMap.put("filename_to_inode_dict", subEntries);
				
			} else {
				dictIndex = parsedFileData.length();
			}
			String[] parts = parsedFileData.substring(0, dictIndex).split(",");
			
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

		public static StringBuffer readFileIntoString(File file) throws IOException {
			BufferedReader buffReader = null;
			StringBuffer fileData = null;
			try {
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
			return fileData;
		}

		public static StringBuffer readFile(String fileName) throws IOException {
			File file = new File(fileName);
			return readFileIntoString(file);
		}

}

public class FileSystemChecker {
	private static List<String> checkerErrors = new ArrayList<String>();
	public static int MAX_BLOCK_SIZE = 4096;
	private static int MAX_POINTERS_IN_BLOCK = 400;
	private static File FILE_SYSTEM_FOLDER;
	
	private FileSystemTree systemTree;

	FileSystemChecker(String fileSystemPath) throws Exception {
		FILE_SYSTEM_FOLDER = new File(fileSystemPath);
		if(!(FILE_SYSTEM_FOLDER.exists() && FILE_SYSTEM_FOLDER.isDirectory())) {
			throw new Exception("Invalid path provided or path is not a directory");
		}
		
		systemTree = null;
	}
	
	public static File getSystemFolder() {
		return FILE_SYSTEM_FOLDER;
	}
	
	public void check() throws Exception {
		File[] entriesInFileSystem = FILE_SYSTEM_FOLDER.listFiles();
		
		if(entriesInFileSystem.length == 0) {
			throw new Exception("No entries in the file system");
		}

		Arrays.sort(entriesInFileSystem, new Comparator<File>() {
			@Override
			public int compare(File f1, File f2) {
				int file1 = Integer.parseInt(f1.getName().substring(9));
				int file2 = Integer.parseInt(f2.getName().substring(9));
				return file1 - file2;
			}
		});

		Map<String, Object> superBlockMap = checkAndGetSuperBlock();
		
		// Initialize root
		Directory root = new Directory((Integer)superBlockMap.get("root"), null);
		if(systemTree == null) {
			systemTree = new FileSystemTree(root);
		} else {
			throw new Exception("File System Tree has already been initiliazed");
		}

		systemTree.parseSystemToBuildTree(entriesInFileSystem);
		
		checkFreeBlockLists((Integer)superBlockMap.get("freestart"),
				(Integer)superBlockMap.get("freeend"),
					(Integer)superBlockMap.get("maxblocks"), entriesInFileSystem);
	}
	
	private void checkFreeBlockLists(int startBlock, int endBlock,
			int maxBlocks, File[] fileSystemFiles) throws IOException {
		// Get number of non-empty blocks
		int numOfBlocksNotEmpty = 0;
		for(File f : fileSystemFiles) {
			if(f.length() > 0)
				numOfBlocksNotEmpty++;
		}
		
		int maxPointers = 400;
		int blockIter = numOfBlocksNotEmpty;
		int fileIter = startBlock;

		while(blockIter < maxBlocks && fileIter <= endBlock) {
			StringBuffer freeBlock = FileSystemUtil.readFileIntoString(fileSystemFiles[fileIter]);
			String[] parts = freeBlock.toString().replaceAll("[\\s]", "").split(",");
			
			//System.out.println("\nFile: " + fileIter + " Length: " + parts.length);
			if(parts.length > maxPointers) {
				checkerErrors.add("Number of pointers in file " + 
						fileSystemFiles[fileIter].getName() + " exceeded");
			}

			for(int j = 0; j< parts.length; j++) {
				int inFile = Integer.parseInt(parts[j]);
				if(inFile == blockIter){
					blockIter = blockIter+1;
				} else {
					checkerErrors.add("Free blocks missing from " + blockIter
							+ " to " + inFile + " in free list block " + 
								fileSystemFiles[fileIter].getName());
					blockIter = Integer.parseInt(parts[j+1]);
				}
			}
			fileIter++;		
		}
	}

	private Map<String, Object> checkAndGetSuperBlock() throws IOException {
		StringBuffer superBlockFile = FileSystemUtil.readFile(FILE_SYSTEM_FOLDER.getAbsolutePath()
				+ "/fusedata.0");
		Map<String, Object> superMap = FileSystemUtil.convertStringToMap(superBlockFile);
		
		// ensure that deviceId = 20
		if((Integer)superMap.get("devid") != 20) {
			checkerErrors.add("Device id is not matching");
		}
		
		checkAllTimesInBlock(superMap);
		return superMap;
	}

	public static void checkAllTimesInBlock(Map<String, Object> blockMap) {
				Long currentTime = System.currentTimeMillis();
				for(String key : blockMap.keySet()) {
					if(key.contains("time")) {
						if((Long)blockMap.get(key) >= currentTime) {
							checkerErrors.add("Time is not in past");
						}
					}
				}
			}

	public void displayAllErrors() {
		for(String error : checkerErrors) {
			System.out.println(error);
		}
	}

	public static void main(String[] args) {
		try {
			FileSystemChecker fsck = new FileSystemChecker(""
					+ "C:/Users/Deepesh/Documents/Deepesh/nyu classes/os/assignments/"
					+ "checker/FS");
			fsck.check();

			// Display all errors in the file system
			fsck.displayAllErrors();
		} catch (Exception e) {
			System.out.println("Exception while checking fileSystem - " + e.getMessage());
		}
	}
}
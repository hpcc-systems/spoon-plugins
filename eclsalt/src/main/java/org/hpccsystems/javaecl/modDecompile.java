package org.hpccsystems.javaecl;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class modDecompile {
	private String outputFolder;
	private boolean isDebug = true;
	public String getOutputFolder() {
		return outputFolder;
	}

	public void setOutputFolder(String outputFolder) {
		this.outputFolder = outputFolder;
	}

	//for testing
	public static void main(String[] args){
		
		modDecompile md = new modDecompile();
		md.setOutputFolder("C:\\dev\\SALTSample\\rawecl\\ProductProfile\\");
		try {  
			md.decompile("C:\\dev\\SALTSample\\ProductProfile.mod");
		} catch (IOException e) {
            e.printStackTrace();
		}   
		
	}
	/*decompile
	 * 
	 * function that runs the show
	 * @accepts String fileName
	 * @returns void
	 */
	private void decompile(String fileName) throws IOException{
		verifyDir();
		openFile(fileName);
	}
	/*verifyDir
	 * 
	 * make sure the outputFolder exists if not create it.
	 * @accepts void
	 * @returns void
	 */
	private void verifyDir(){
		//make sure the outputFolder exists if not create it.
		
		File theDir = new File(outputFolder);

		  // if the directory does not exist, create it
		  if (!theDir.exists())
		  {
		    System.out.println("creating directory: " + outputFolder);
		    boolean result = theDir.mkdir();  
		    if(result && isDebug){    
		       System.out.println("DIR created");  
		     }

		  }


	}
	/*writeFile
	 * 
	 * @accepts: String fileName, String contents
	 * @returns void
	 */
	private void writeFile(String fileName, String contents) throws IOException{
		    if(isDebug){
		    	System.out.println("writing file: '" + outputFolder + fileName + "'");
		    }
            BufferedWriter out = new BufferedWriter(new FileWriter(outputFolder + fileName));
            out.write(contents);
            out.close();
        
	}
	
	/*openFile
	 * 
	 * @accepts String fileName
	 * @returns void
	 */
	private void openFile(String fileName) throws IOException{
	      String outStr = "";

	          // Open the file that is the first 
	          // command line parameter
	          FileInputStream fstream = new FileInputStream(fileName);
	          // Get the object of DataInputStream
	          DataInputStream in = new DataInputStream(fstream);
	          BufferedReader br = new BufferedReader(new InputStreamReader(in));
	          String strLine;

	          int length = 0;
	          boolean first = true;
	          //Read File Line By Line
	          String thisFileName = "";
	          while ((strLine = br.readLine()) != null)   {

	        	  //example line "//Import:ProductProfile.GenerationDocs"
	              //find lines that start with //Import:
	              if(strLine.length()>=10 && strLine.substring(0, 9).equalsIgnoreCase("//Import:")){
	            	  System.out.println(strLine.substring(0, 9));
	            	  //split and write the next file if outStr != ""
	            	  String fileOutName = "temp.ecl";
	            	  // the first . in the line is the start of the intended file name
	            	  int start = strLine.indexOf('.')+1;
	            	  //end of the line we have the file name
	            	  int end = strLine.length()-1;
	            	  //get the filename from string
	            	  fileOutName = strLine.substring(start,end);
	            	  fileOutName += ".ecl";
	            	  if(!outStr.equals("") && !thisFileName.equals("")){
	            		  //we have existing data write it to the file
	            		  writeFile(thisFileName, outStr);
	            	  }
	            	  thisFileName = fileOutName;
	            	  outStr = "";
	              }else{
	            	  outStr += strLine + "\r\n";
	              }
	          }
	          //need to write last file
	          if(!outStr.equals("") && !thisFileName.equals("")){
        		  //we have existing data write it to the file
        		  writeFile(thisFileName, outStr);
        	  }
	          in.close();
	      
	  }

}

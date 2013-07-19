/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.saltinternallinking;



import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.swt.widgets.Text;
import org.hpccsystems.javaecl.SaltDataProfilingAPI;
import org.hpccsystems.javaecl.SaltInternalLinking;
import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;
import org.hpccsystems.ecljobentrybase.*;

import com.hpccsystems.salt.helpers.DatasetNode;
import com.hpccsystems.salt.helpers.LoadSpecificties;
import org.hpccsystems.salt.hygiene.Generate;
/**
 *
 * @author ChambersJ
 */
public class SALTInternalLinking extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    
	
    private String hygieneOutputFolder = "";
    private String specificitiesOutputFolder = "";
    private String doAgain = "no";
    private String iteration = "1";

    public String getHygieneOutputFolder() {
		return hygieneOutputFolder;
	}

	public void setHygieneOutputFolder(String hygieneOutputFolder) {
		this.hygieneOutputFolder = hygieneOutputFolder;
	}

	public String getSpecificitiesOutputFolder() {
		return specificitiesOutputFolder;
	}

	public void setSpecificitiesOutputFolder(String specificitiesOutputFolder) {
		this.specificitiesOutputFolder = specificitiesOutputFolder;
	}

	public String getDoAgain() {
		return doAgain;
	}

	public void setDoAgain(String doAgain) {
		this.doAgain = doAgain;
	}

	public String getIteration() {
		return iteration;
	}

	public void setIteration(String iteration) {
		this.iteration = iteration;
	}

	@Override
    public Result execute(Result prevResult, int k) throws KettleException {
        
        Result result = prevResult;
        String SALTPath = "";
        AutoPopulate ap = new AutoPopulate();
        String jobNameNoSpace = "";
        JobMeta jobMeta = super.parentJob.getJobMeta();
        System.out.println("Start Internal Linking");
        String outputDir = "";
        try{
        	outputDir = ap.getGlobalVariable(jobMeta.getJobCopies(),"file_name");
        	String jobName = ap.getGlobalVariable(jobMeta.getJobCopies(),"jobName");
            jobNameNoSpace = jobName.replace(" ", "_");  
            SALTPath = ap.getGlobalVariable(jobMeta.getJobCopies(),"SALTPath");
        }catch (Exception e){
        	System.out.println("Error Parsing existing OutputDir ");
            System.out.println(e.toString());
            e.printStackTrace();
            
        }
        //System.out.println("output dir: " + outputDir);
        LoadSpecificties ls = new LoadSpecificties();
        ls.setModuleName(jobNameNoSpace+"module");
        if(!this.iteration.equalsIgnoreCase("1")){
        	ls.setIdExists(true);
        }
        ArrayList<DatasetNode> ds = ls.buildLinkingXML(outputDir + "\\",
														specificitiesOutputFolder,
														hygieneOutputFolder);
		//System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
		//System.out.println(outputDir + " : " +
		//		specificitiesOutputFolder + " : " +
		//		hygieneOutputFolder);
        
		/*
		ArrayList<DatasetNode> ds = ls.buildLinkingXML("C:/Spoon Demos/new/salt/out_internal_clustering/",
				"C:/Spoon Demos/new/salt/out_specificities/",
				"C:/Spoon Demos/new/salt/out_hygine/");
				 */
		SaltInternalLinking sl = new SaltInternalLinking();
		sl.setDatasetName(ls.getFileName());
		sl.setDoAgain(doAgain.equalsIgnoreCase("yes"));
		sl.setIteration(iteration);
		sl.setName(this.getName());
		sl.setSaltLib(jobNameNoSpace + "module");
		
		//String dsECL = System.getProperties().getProperty("Dataset-" + ls.getDatasetName());
		String dsECL = ls.getEclDatasetCode();
		//String ecl = dsECL + "" + sl.ecl();
		String ecl = sl.ecl();
        
        logBasic("{----SALT INTERNAL LINKING----}");
        logBasic("{SALT INTERNAL LINKING Job} Execute = " + ecl);
        
        logBasic("{SALT INTERNAL LINKING Job} Previous =" + result.getLogText());
        
        result.setResult(true);
        
        RowMetaAndData data = new RowMetaAndData();
        data.addValue("ecl", Value.VALUE_TYPE_STRING, ecl);
        
        
        List list = result.getRows();
        list.add(data);
        String eclCode = parseEclFromRowData(list);
        /*
        String eclCode = "";
        if (list == null) {
            list = new ArrayList();
        } else {
            
            for (int i = 0; i < list.size(); i++) {
                RowMetaAndData rowData = (RowMetaAndData) list.get(i);
                String code = rowData.getString("ecl", null);
                if (code != null) {
                    eclCode += code;
                }
            }
            logBasic("{Dataset Job} ECL Code =" + eclCode);
        }
        */
        result.setRows(list);
        result.setLogText("ECLDataset executed, ECL code added");
        
        buildSaltSpec(SALTPath, outputDir,jobNameNoSpace);
        try{
        	String dsEcl = ls.getRsDef();
        	//System.out.println(dsEcl);
        	//int endIndex = dsEcl.lastIndexOf("end;");		
        	//System.out.println("Index of (end;): " + endIndex);
        	//dsEcl = dsEcl.substring(0, endIndex+4);
        	
        	//dsEcl = dsEcl.replace("end;", "INTEGER spoonRecordID;\r\nend;");
        	//dsEcl = dsEcl.replace("end;", "UNSIGNED6 spoonRecordID;\r\nend;");
        	
        	BufferedWriter out = new BufferedWriter(new FileWriter(outputDir + "\\" + jobNameNoSpace + "module\\layout_spoon_" + ls.getFileName() + ".ecl"));
        	out.write("EXPORT layout_spoon_" + dsEcl);
        	out.close();
        	//build a recordset with the spoon id's
        	dsEcl = dsEcl.replace("end;", "UNSIGNED6 spoonClusterID;\r\nUNSIGNED6 spoonRecordID;\r\nend;");
        	BufferedWriter out2 = new BufferedWriter(new FileWriter(outputDir + "\\" + jobNameNoSpace + "module\\layout_" + ls.getFileName() + ".ecl"));
        	out2.write("EXPORT layout_" + dsEcl);
        	
        	out2.close();
        }catch(Exception e){
        	
        	System.out.println("****************************************");
        	System.out.println(e.toString());
        }
        
        try{
        	String dsEcl = "";//"IMPORT layout_ProductsRS;\r\n";//ls.getRsDef();
        	
        	//need to add DS dec to xml
        	dsEcl += "inputData_" + ls.getDsName() + " := dataset('" + ls.getLogicalFile() + "',layout_spoon_" + ls.getRecordName() + "," + ls.getFileType() + ");" + "\r\n";
        	
        	/*
        	 * "transform(recordof(layout_" + ls.getRecordName() + ")," +
							         " self.spoonRecordID := 0," +
							         " self.spoonClusterID := 0," +
									 " self := left));";
        	 */
        	int intIteration = Integer.parseInt(this.iteration);
        	if(intIteration == 1 ){
        	String transform = "layout_"+ ls.getRecordName() + " SpoonTransform(inputData_" + ls.getDsName() + " L) := TRANSFORM\r\n";
        	transform += "SELF.spoonClusterID := 0;\r\n";
        	transform += "SELF.spoonRecordID := 0;\r\n";
        	transform += "SELF := L;\r\n";
        	transform += "END;\r\n\r\n";
        		
        	dsEcl += transform;
        	dsEcl += "In_" + ls.getDsName() + " := project(inputData_" + ls.getDsName() + ",SpoonTransform(LEFT));\r\n"; 
                            
        	
        	String iterator = "layout_" + ls.getRecordName() + " AddIds(layout_"+ ls.getRecordName() + " L, layout_" + ls.getRecordName() + " R) := TRANSFORM\r\n" +
        						"SELF.spoonClusterID := L.spoonRecordID + 1;\r\n" +
        						"SELF.spoonRecordID := L.spoonRecordID + 1;\r\n" +
        						"SELF := R;\r\n" +
        						"END;\r\n"+
        						"EXPORT In_" + ls.getRecordName() +" := ITERATE(In_" + ls.getDsName() + ",AddIds(LEFT,RIGHT));\r\n\r\n";
        	dsEcl = dsEcl + iterator;
        	}else if (intIteration > 1){
        		//non primary iteration
        		dsEcl += "EXPORT In_" + ls.getRecordName() + " := dataset('~temp::spoonClusterID::" + jobNameNoSpace + "module::it" + (intIteration-1) + "', Layout_" + ls.getRecordName()+ ", flat);\r\n";
        	}else{
        		//unknown state trow error
        		
        	}
        	//dsEcl += "EXPORT In_" + ls.getRecordName() + " := inputData_" + ls.getDsName() + ";";
        	BufferedWriter out = new BufferedWriter(new FileWriter(outputDir + "\\" + jobNameNoSpace + "module\\In_" + ls.getFileName() + ".ecl"));
        	out.write(dsEcl);
        	out.close();
        }catch(Exception e){
        	
        	//System.out.println("****************************************");
        	System.out.println(e.toString());
        }
        //System.out.println("end Internal Linking");
        return result;
    }
	
	private boolean buildSaltSpec(String saltPath, String outputDir, String jobNameNoSpace){
		Boolean result = true;
		
		try {              
            

    		Generate gen = new Generate();
    		String spec = gen.generateHygieneSpecFromXMLFile(outputDir + "\\salt.xml");
    		
    		BufferedWriter out2 = new BufferedWriter(new FileWriter(outputDir + "\\salt.spc"));
            out2.write(spec);
            out2.close();
           
    		
            String modFile = "";
            //System.out.println("-------------------------------------SALT COMPILE2--------------------------------");
            boolean compileSuccess = compileSalt(saltPath, outputDir + "\\salt.spc", outputDir + "",jobNameNoSpace);
            
            if(!compileSuccess){
            	 String SaltError = "Unable to create the SALT files! Please check your salt path in Global Variables, and your output path in Execute.";
            	 
             	 logError(SaltError);
             	 System.out.println(SaltError);
             	 result = false;
             	 return false;
            }else{
            	logBasic("Salt Compiled");
            	//System.out.println("Salt Compiled moving on");
            }
            

    		
        } catch (IOException e) {
             e.printStackTrace();
             String SaltError = "Unable to create the SALT files! Please check your salt path in Global Variables, and your output path in Execute.";
        	 
         	 logError(SaltError);
         	 System.out.println(SaltError);
         	 result = false;
         	 return false;
        } 
		//boolean compileSuccess = compileSalt(saltPath, outputDir + "\\salt.spc", outputDir + "",jobNameNoSpace);
		return result;
	}
	
	public boolean compileSalt(String saltPath, String spcFile, String outputDir, String jobNameNoSpace){
		boolean saltExists = (new File(saltPath + "\\salt.exe")).exists();
		boolean outExists = (new File(outputDir).exists());
		boolean success = false;
		if(saltExists && outExists){
			String cmd = "\"" + saltPath + "\\salt.exe\" -ga -D\"" + outputDir + "\" \"" + spcFile + "\"";
			//System.out.println("-->" + cmd + "<--");
		 	try{
				 System.out.println("runtime");
				 File path = new File(saltPath);
				 Runtime rt = java.lang.Runtime.getRuntime();
				 Process p = rt.exec(cmd, null, path);
				 
				//block until salt compile is completed
	                int b = 0;
	                while(!(new File(outputDir + "\\" + jobNameNoSpace + "module")).exists() && b < 150){
	                	Thread.sleep(1000);
	                	b++;
	                }
	                
				// if(p.exitValue() == 0){
					 if((new File(outputDir + "\\" + jobNameNoSpace + "module")).exists()){
						 success = true;
					 }
				// }
				 
				 
                    
		 	}catch (Exception e){
	            System.out.println(e.toString());
	            e.printStackTrace();
	            //error += "Couldn't Locate the Salt Install and/or the output Directory Please Verify settings!";
	            return false;
	        }
		}else{
			//salt install doesn't exist
			//if(!saltExists){error += "Salt not found in provided path, please check Global Variables! \r\n";}
			//if(!outExists){error += "Output Directory defined in Execute step doesn't exist";}
			success = false;
		}
		
		return success;
	}
 
    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);

            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "hygieneOutputFolder")) != null)
            	setHygieneOutputFolder(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "hygieneOutputFolder")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "specificitiesOutputFolder")) != null)
            	setSpecificitiesOutputFolder(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "specificitiesOutputFolder")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "doAgain")) != null)
            	setDoAgain(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "doAgain")));
            if(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "iteration")) != null)
            	setIteration(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "iteration")));

        } catch (Exception e) {
            throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();
        
        retval += "		<hygieneOutputFolder><![CDATA[" + hygieneOutputFolder + "]]></hygieneOutputFolder>" + Const.CR;
        retval += "		<specificitiesOutputFolder><![CDATA[" + specificitiesOutputFolder + "]]></specificitiesOutputFolder>" + Const.CR;
        retval += "		<doAgain><![CDATA[" + doAgain + "]]></doAgain>" + Const.CR;
        retval += "		<iteration><![CDATA[" + iteration + "]]></iteration>" + Const.CR;
        
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
        	
            if(rep.getStepAttributeString(id_jobentry, "hygieneOutputFolder") != null)
            	hygieneOutputFolder = rep.getStepAttributeString(id_jobentry, "hygieneOutputFolder"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "specificitiesOutputFolder") != null)
            	specificitiesOutputFolder = rep.getStepAttributeString(id_jobentry, "specificitiesOutputFolder"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "doAgain") != null)
            	doAgain = rep.getStepAttributeString(id_jobentry, "doAgain"); //$NON-NLS-1$
            if(rep.getStepAttributeString(id_jobentry, "iteration") != null)
            	iteration = rep.getStepAttributeString(id_jobentry, "iteration"); //$NON-NLS-1$
        
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
        	/*    	private String hygieneOutputFolder;
    				private String specificitiesOutputFolder;
        	 */
            rep.saveStepAttribute(id_job, getObjectId(), "hygieneOutputFolder", hygieneOutputFolder); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "specificitiesOutputFolder", specificitiesOutputFolder); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "doAgain", doAgain); //$NON-NLS-1$
            rep.saveStepAttribute(id_job, getObjectId(), "iteration", iteration); //$NON-NLS-1$
            
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

    public boolean evaluates() {
        return true;
    }

    public boolean isUnconditional() {
        return true;
    }
}

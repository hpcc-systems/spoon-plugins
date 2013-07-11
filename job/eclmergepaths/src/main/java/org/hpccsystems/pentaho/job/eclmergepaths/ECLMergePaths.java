/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclmergepaths;

import java.util.ArrayList;
import java.util.List;
import org.hpccsystems.javaecl.Output;
import org.hpccsystems.javaecl.EclDirect;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;
import org.hpccsystems.javaecl.Column;
import java.io.*;
import org.hpccsystems.ecljobentrybase.*;


/**
 *
 * @author ChambersJ
 */
public class ECLMergePaths extends ECLJobEntry{//extends JobEntryBase implements Cloneable, JobEntryInterface {
    

    private String numberOfPaths = "1";
    private static boolean isFirst = true; 
    private static int numberOfPathsStat = 0;
    private static int numberOfPathsRemaining = 0;
    private static Result globalResult;
    
    public static boolean isReady = false;
    public static boolean isValid = false;


    public String getNumberOfPaths() {
        return numberOfPaths;
        
    }

    public void setNumberOfPaths(String numberOfPaths) {
        this.numberOfPaths = numberOfPaths;
        ECLMergePaths.numberOfPathsStat = Integer.parseInt(numberOfPaths);
        ECLMergePaths.numberOfPathsRemaining = Integer.parseInt(numberOfPaths);
        globalResult = new Result();
    }
    
   

    
    
    

    @Override
    public Result execute(Result prevResult, int k) throws KettleException {
        //this needs to be updated, it works but not well.
       logBasic("isFirst =" + ECLMergePaths.isFirst);
       
       ECLMergePaths.isFirst = false;
       
      // logBasic("Number of Paths Global =" + this.getVariable("numberOfPathsGlobal"));
       Result result = prevResult;
       
       logBasic("Number of Paths Remaining =" + ECLMergePaths.numberOfPathsRemaining);
       ECLMergePaths.numberOfPathsRemaining--;
       
         //globalResult.add(result);
        RowMetaAndData data = new RowMetaAndData();
        List list = result.getRows();
        List listGlobal = globalResult.getRows();
        listGlobal.addAll(list);
        result.setRows(listGlobal);
        
       if(ECLMergePaths.numberOfPathsRemaining != 0){
           result.setStopped(true);
           isValid = false;
           //result.setResult(false);
           
           //try to find exitstatus documentation
           //result.setExitStatus(5);
           
           //globalResult.setRows(list);
           globalResult = result;
           logBasic("Not last job --------------------------");
           //this.clear();
          // this.parentJob.stopAll();
         
           //return new Result();
           //while(!ECLMergePaths.isReady){
               //delay this thread until the last to finish finishes
               
           //}
           ECLMergePaths.isReady = false;
           int count = 0;
           while(count < 10000){
               //add a delay for non primary path.
               count++;
           }
       }else{
    	   isValid = true;
           result.setStopped(false);
           result.setResult(true);
           globalResult = result;
           logBasic("Is last job --------------------------");
           //globalResult.add(result);
           //result = globalResult;
           List list2 = result.getRows();
           String eclCode = parseEclFromRowData(list);
           /*
            String eclCode = "";
            if (list2 == null) {
                list2 = new ArrayList();
            } else {

                for (int i = 0; i < list2.size(); i++) {
                    RowMetaAndData rowData = (RowMetaAndData) list2.get(i);
                   logBasic("RAW DATA =" + rowData.getData().toString());
                    String code = rowData.getString("ecl", null);
                    if (code != null) {
                        eclCode += code;
                    }
                }
                logBasic("{Merge Paths Job} ECL Code =" + eclCode);
            }
*/
            //result.setRows(list);
            ECLMergePaths.isReady = true;
            
            ECLMergePaths.numberOfPathsRemaining = ECLMergePaths.numberOfPathsStat;
            //Result globalResult;
           
       }
           
        
         
        
        return globalResult;
    }

    @Override
    public void loadXML(Node node, List<DatabaseMeta> list, List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
        try {
            super.loadXML(node, list, list1);

            setNumberOfPaths(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "number_of_paths")));
           

        } catch (Exception e) {
            throw new KettleXMLException("ECL Output Job Plugin Unable to read step info from XML node", e);
        }

    }

    public String getXML() {
        String retval = "";
        
        retval += super.getXML();

        retval += "		<number_of_paths>" + numberOfPaths + "</number_of_paths>" + Const.CR;
       
        return retval;

    }

    public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
            throws KettleException {
        try {
          
            numberOfPaths = rep.getStepAttributeString(id_jobentry, "numberOfPaths"); //$NON-NLS-1$
                 
        } catch (Exception e) {
            throw new KettleException("Unexpected Exception", e);
        }
    }

    public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
        try {
       
            rep.saveStepAttribute(id_job, getObjectId(), "numberOfPaths", numberOfPaths); //$NON-NLS-1$
               
        } catch (Exception e) {
            throw new KettleException("Unable to save info into repository" + id_job, e);
        }
    }

    public boolean evaluates() {
        return isValid;
    }

    public boolean isUnconditional() {
        return false;
    }
}

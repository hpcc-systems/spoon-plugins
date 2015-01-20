/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecltstat;

import java.util.List;

import org.hpccsystems.ecljobentrybase.ECLJobEntry;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

/**
 * 
 * @author KeshavS
 */
public class ECLTStat extends ECLJobEntry {// extends JobEntryBase implements
											// Cloneable, JobEntryInterface {

	private String datasetName = "";
	private String X1 = "";
	private String X2 = "";
	private String paired = "";
	private String Variance = "";
	private String Mew = "";
			
	
	
	public String getMew() {
		return Mew;
	}

	public void setMew(String mew) {
		this.Mew = mew;
	}

	public String getVariance() {
		return Variance;
	}

	public void setVariance(String variance) {
		this.Variance = variance;
	}

	public String getPaired() {
		return paired;
	}

	public void setPaired(String paired) {
		this.paired = paired;
	}

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public String getX1() {
		return X1;
	}

	public void setX1(String x1) {
		this.X1 = x1;
	}

	public String getX2() {
		return X2;
	}

	public void setX2(String x2) {
		this.X2 = x2;
	}

	@Override
	public Result execute(Result prevResult, int k) throws KettleException {
		Result result = modifyResults(prevResult);
        if(result.isStopped()){
        return result;
       }
        else {
			String tstat = "";
			if(!X2.equalsIgnoreCase("0")){
				if(getPaired().equalsIgnoreCase("Unpaired")){
					tstat = "TRec := RECORD\n	m1 := AVE(GROUP,"+getDatasetName()+"."+X1+");\n	m2 := AVE(GROUP,"+getDatasetName()+"."+X2+");\n" +
								   "	var1 := VARIANCE(GROUP,"+getDatasetName()+"."+X1+");\n"+
								   "	var2 := VARIANCE(GROUP,"+getDatasetName()+"."+X2+");\n" +
								   "	n1 := COUNT(GROUP,"+getDatasetName()+"."+X1+">0 OR "+getDatasetName()+"."+X1+"<=0);\n" +
								   "	n2 := COUNT(GROUP,"+getDatasetName()+"."+X2+">0 OR "+getDatasetName()+"."+X2+"<=0);\nEND;\n";
					
					tstat += "tab := TABLE("+getDatasetName()+",TRec);\n";
					if(getVariance().equalsIgnoreCase("Equal Variance")){
						tstat += "TReX := RECORD\n	REAL Tstat;\n REAL DF;\nEND;\n";
						tstat += "TReX Trans(tab L, INTEGER C) := TRANSFORM\n	" +
								 "	REAL var := SQRT((((L.n1 - 1)*L.var1) + ((L.n2 - 1)*L.var2))/(L.n1+L.n2-2));\n"+
								 "	REAL siz := SQRT((1/L.n1)+(1/L.n2));\n	" +
								 "	SELF.Tstat := ABS((L.m1 - L.m2)/(  var  *  siz  ));\n" +
								 "	SELF.DF := L.n1 + L.n2 -2;\nEND;\n";
						
						
						tstat += "Stat := PROJECT(tab,Trans(LEFT,COUNTER));\n";
						tstat += "OUTPUT(Stat,NAMED('TStatistic'));\n";
					}
					else {
						tstat += "TReX := RECORD\n	REAL Tstat;\n REAL DF;\nEND;\n";
						tstat += "TReX Trans(tab L, INTEGER C) := TRANSFORM\n	" +
								 "	REAL var := SQRT((L.var1/L.n1) + (L.var2/L.n2));\n"+						
								 "	SELF.Tstat := ABS((L.m1 - L.m2)/(  var  ));\n" +
								 "	REAL num := ((L.var1/L.n1) + (L.var2/L.n2))*((L.var1/L.n1) + (L.var2/L.n2));\n" +
								 "	REAL den := ((L.var1/L.n1)*(L.var1/L.n1)/(L.n1 - 1)) + ((L.var2/L.n2)*(L.var2/L.n2)/(L.n2 - 1));\n" +
								 "	SELF.DF := num/den;\nEND;\n";
						
						
						tstat += "Stat := PROJECT(tab,Trans(LEFT,COUNTER));\n";
						tstat += "OUTPUT(Stat,NAMED('TStatistic'));\n";
					}
				}
				else{
					tstat = "TRec := RECORD\n	m1 := AVE(GROUP,"+getDatasetName()+"."+X1+"-"+getDatasetName()+"."+X2+" );\n" +
							   "	var1 := VARIANCE(GROUP,"+getDatasetName()+"."+X1+"-"+getDatasetName()+"."+X2+");\n"+							   
							   "	n1 := COUNT(GROUP,"+getDatasetName()+"."+X1+">0 OR "+getDatasetName()+"."+X1+"<=0);\nEND;\n";
					
					tstat += "tab := TABLE("+getDatasetName()+",TRec);\n";
					tstat += "TReX := RECORD\n	REAL Tstat;\n REAL DF;\nEND;\n";
					tstat += "TReX Trans(tab L, INTEGER C) := TRANSFORM\n	" +
							 "	SELF.Tstat := ABS((L.m1 - "+getMew()+")/(  SQRT((L.var1)  *  SQRT(1/L.n1)  )));\n" +
							 "	SELF.DF := L.n1 - 1;\nEND;\n";
				}
			}
			else{
				tstat = "TRec := RECORD\n	m1 := AVE(GROUP,"+getDatasetName()+"."+X1+");\n	m2 := 0;\n" +
						   "	var1 := VARIANCE(GROUP,"+getDatasetName()+"."+X1+");\n"+						   
						   "	n1 := COUNT(GROUP,"+getDatasetName()+"."+X1+">0 OR "+getDatasetName()+"."+X1+"<=0);\nEND;\n";
						   
			
				tstat += "tab := TABLE("+getDatasetName()+",TRec);\n";
			
				tstat += "TReX := RECORD\n	REAL Tstat;\n REAL DF;\nEND;\n";
				tstat += "TReX Trans(tab L, INTEGER C) := TRANSFORM\n	" +
						 "	SELF.Tstat := ABS((L.m1 - L.m2)/( SQRT((L.var1)  *  SQRT(1/L.n1)  )));\n" +
						 "	SELF.DF := L.n1 - 1;\nEND;\n";
				
				
				tstat += "Stat := PROJECT(tab,Trans(LEFT,COUNTER));\n";
				tstat += "OUTPUT(Stat,NAMED('TStatistic'));\n";
			
			}
			
			logBasic("Tstat Job =" + tstat);

			result.setResult(true);

			RowMetaAndData data = new RowMetaAndData();
			data.addValue("ecl", Value.VALUE_TYPE_STRING, tstat);

			List list = result.getRows();
			list.add(data);
			String eclCode = parseEclFromRowData(list);
			result.setRows(list);
			result.setLogText("ECLTStat executed, ECL code added");
			return result;
		}

	}

	@Override
	public void loadXML(Node node, List<DatabaseMeta> list,List<SlaveServer> list1, Repository rpstr) throws KettleXMLException {
		try {
			super.loadXML(node, list, list1);

			if (XMLHandler.getNodeValue(XMLHandler.getSubNode(node,	"dataset_name")) != null)
				setDatasetName(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "dataset_name")));
			if (XMLHandler.getNodeValue(XMLHandler.getSubNode(node,	"X1")) != null)
				setX1(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "X1")));
			if (XMLHandler.getNodeValue(XMLHandler.getSubNode(node,	"X2")) != null)
				setX2(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "X2")));
			if (XMLHandler.getNodeValue(XMLHandler.getSubNode(node,	"Paired")) != null)
				setPaired(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Paired")));
			if (XMLHandler.getNodeValue(XMLHandler.getSubNode(node,	"Variance")) != null)
				setVariance(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Variance")));
			if (XMLHandler.getNodeValue(XMLHandler.getSubNode(node,	"Mew")) != null)
				setMew(XMLHandler.getNodeValue(XMLHandler.getSubNode(node, "Mew")));

		} catch (Exception e) {
			throw new KettleXMLException("ECL Dataset Job Plugin Unable to read step info from XML node",e);
		}

	}

	public String getXML() {
		String retval = "";

		retval += super.getXML();

		retval += "		<dataset_name ><![CDATA[" + datasetName	+ "]]></dataset_name>" + Const.CR;
		retval += "		<X1><![CDATA[" + X1	+ "]]></X1>" + Const.CR;
		retval += "		<X2><![CDATA[" + X2	+ "]]></X2>" + Const.CR;
		retval += "		<Paired><![CDATA[" + paired	+ "]]></Paired>" + Const.CR;
		retval += "		<Variance><![CDATA[" + Variance	+ "]]></Variance>" + Const.CR;
		retval += "		<Mew><![CDATA[" + Mew	+ "]]></Mew>" + Const.CR;
		return retval;

	}

	public void loadRep(Repository rep, ObjectId id_jobentry,List<DatabaseMeta> databases, List<SlaveServer> slaveServers) throws KettleException {
		try {
			if (rep.getStepAttributeString(id_jobentry, "datasetName") != null)
				datasetName = rep.getStepAttributeString(id_jobentry,"datasetName"); //$NON-NLS-1$
			if (rep.getStepAttributeString(id_jobentry, "X1") != null)
				X1 = rep.getStepAttributeString(id_jobentry,"X1"); //$NON-NLS-1$
			if (rep.getStepAttributeString(id_jobentry, "X2") != null)
				X2 = rep.getStepAttributeString(id_jobentry,"X2"); //$NON-NLS-1$
			if (rep.getStepAttributeString(id_jobentry, "Paired") != null)
				paired = rep.getStepAttributeString(id_jobentry,"Paired"); //$NON-NLS-1$
			if (rep.getStepAttributeString(id_jobentry, "Variance") != null)
				Variance = rep.getStepAttributeString(id_jobentry,"Variance"); //$NON-NLS-1$
			if (rep.getStepAttributeString(id_jobentry, "Mew") != null)
				Mew = rep.getStepAttributeString(id_jobentry,"Mew"); //$NON-NLS-1$
			
		} catch (Exception e) {
			throw new KettleException("Unexpected Exception", e);
		}
	}

	public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
		try {
			rep.saveStepAttribute(id_job, getObjectId(),"datasetName", datasetName); //$NON-NLS-1$
			rep.saveStepAttribute(id_job, getObjectId(),"X1", X1); //$NON-NLS-1$
			rep.saveStepAttribute(id_job, getObjectId(),"X2", X2); //$NON-NLS-1$
			rep.saveStepAttribute(id_job, getObjectId(),"Paired", paired); //$NON-NLS-1$
			rep.saveStepAttribute(id_job, getObjectId(),"Variance", Variance); //$NON-NLS-1$
			rep.saveStepAttribute(id_job, getObjectId(),"Mew", Mew); //$NON-NLS-1$
		} catch (Exception e) {
			throw new KettleException("Unable to save info into repository"+ id_job, e);
		}
	}

	public boolean evaluates() {
		return true;
	}

	public boolean isUnconditional() {
		return true;
	}

}

package org.hpccsystems.pentaho.job.eclfstatistic;
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import java.util.ArrayList;

/**
 *
 * @author ChalaAX
 */
public class FStatistic {
	

    private String name;
    private String datasetName;
    private ArrayList<String> fields;//Comma separated list of fieldNames. a "-" prefix to the field name will indicate descending order
    
    private boolean runLocal;
    
    public FStatistic() {
		// TODO Auto-generated constructor stub
    	Storage.noOfVariables++;
	}

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public ArrayList<String> getFields() {
        return fields;
    }

    public void setFields(ArrayList<String> fields) {
        this.fields = fields;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isRunLocal() {
        return runLocal;
    }

    public void setRunLocal(boolean runLocal) {
        this.runLocal = runLocal;
    }

    public String ecl() {
    	int noOfVar = Storage.noOfVariables;
        String ecl ="";
        Mean eclUnvariate = new Mean();
        eclUnvariate.setDatasetName(this.getDatasetName());
        eclUnvariate.setCheckList("true,fa,fa,fa,af,af,af");
        String fieldList = "";
        for (int i = 0; i < fields.size(); i++) {
			if(i==0)
				fieldList = fields.get(0);
			else {
				fieldList = fieldList + "," + fields.get(i);
			}
		}
        eclUnvariate.setFieldList(fieldList);
        eclUnvariate.setPeople(fields);
        eclUnvariate.setSingle("MeanForFStatistic"+noOfVar);
        ecl = eclUnvariate.ecl() + "\n";
        ArrayList<String> asd = new ArrayList<String>();
        eclUnvariate = new Mean();
        eclUnvariate.setCheckList("true,fa,fa,fa,af,af,af");
        eclUnvariate.setDatasetName("MeanForFStatistic"+noOfVar);
        eclUnvariate.setFieldList("mean");
        asd.clear();
        asd.add("mean");
        eclUnvariate.setPeople(asd);
        eclUnvariate.setSingle("GlobalMeanForFStatistic"+noOfVar);
        ecl += eclUnvariate.ecl() + "\n";
        
        ecl += "count"+noOfVar+" := COUNT("+datasetName+");\n";
        ecl += "MeanForFStatistic"+noOfVar+" transFunc"+noOfVar+
        		"(MeanForFStatistic"+noOfVar+" xyz,DECIMAL40_10 abc) := TRANSFORM\n";
        ecl += "\tSELF.mean := (abc-xyz.mean)*(abc-xyz.mean);\n";
        ecl += "\tSELF.FIELD := xyz.field;\nEND;\n";
        
        ecl += "normMeans"+noOfVar+" := PROJECT(MeanForFStatistic"+noOfVar+"," +
        		"transFunc"+noOfVar+"(LEFT,GlobalMeanForFStatistic"+noOfVar+"[1].mean));\n";
        eclUnvariate = new Mean();
        eclUnvariate.setCheckList("true,fa,fa,fa,af,af,af");
        eclUnvariate.setDatasetName("normMeans"+noOfVar);
        eclUnvariate.setFieldList("mean");
        asd.clear();
        asd.add("mean");
        eclUnvariate.setPeople(asd);
        eclUnvariate.setSingle("sumOfMeanSqaureDifferences"+noOfVar);
        ecl += eclUnvariate.ecl() + "\n";
        ecl += "noOfGroups"+noOfVar+" := COUNT(MeanForFStatistic"+noOfVar+");\n";
        ecl += "BtwGrpSOSbefore"+noOfVar+" := sumOfMeanSqaureDifferences"+noOfVar+"[1].mean * count"+noOfVar+
        		" * noOfGroups"+noOfVar+";\n";
        ecl += "BtwGrpSOS"+noOfVar+" := BtwGrpSOSbefore"+noOfVar+" / (noOfGroups"+noOfVar+" -1);\n";
        
        
        ecl += "newRecordwithReqVariables"+noOfVar+" := RECORD\n";
        for (int i = 0; i < fields.size(); i++) {
			ecl += "\tDECIMAL40_10 "+fields.get(i)+";\n";
		}
        ecl += "END;\n";
        
        ecl += "newRecordwithReqVariables"+noOfVar+" newTransFunc"+noOfVar+"("+datasetName+" asdf";
        for (int i = 0; i < fields.size(); i++) {
			ecl += ",DECIMAL40_10 mean"+i;
		}
        ecl += ") := TRANSFORM\n";
        for (int i = 0; i < fields.size(); i++) {
			ecl += "\tSELF."+fields.get(i)+" := (asdf."+fields.get(i)+" - mean"+i+")*(asdf."+fields.get(i)+" - mean"+i+");\n";
		}
        ecl += "END;\n";
        ecl += "listOfSquaredErrors"+noOfVar+" := PROJECT("+datasetName+",newTransFunc"+noOfVar+"(LEFT";
        for (int i = 0; i < fields.size(); i++) {
			ecl += ",MeanForFStatistic"+noOfVar+"["+i+"].mean";
		}
        ecl += "));\n";
        
        
        eclUnvariate = new Mean();
        eclUnvariate.setCheckList("true,fa,fa,fa,af,af,af");
        eclUnvariate.setDatasetName("listOfSquaredErrors"+noOfVar);
        eclUnvariate.setFieldList(fieldList);
        eclUnvariate.setPeople(fields);
        eclUnvariate.setSingle("listOfSquaredErrorsMeanOfEveryColumn"+noOfVar);
        ecl += eclUnvariate.ecl();
        
        eclUnvariate = new Mean();
        eclUnvariate.setCheckList("true,fa,fa,fa,af,af,af");
        eclUnvariate.setDatasetName("listOfSquaredErrorsMeanOfEveryColumn"+noOfVar);
        eclUnvariate.setFieldList("mean");
        eclUnvariate.setPeople(asd);
        eclUnvariate.setSingle("listOfSquaredErrorsMeanAll"+noOfVar);
        ecl += eclUnvariate.ecl();
        
        ecl += "dof"+noOfVar+" := noOfGroups"+noOfVar+" * (count"+noOfVar+" -1);\n";
        ecl += "withinGroupMeanSqrError"+noOfVar+" := listOfSquaredErrorsMeanAll"+noOfVar+"[1].mean /dof"+noOfVar+";\n";
        ecl += getName()+" := BtwGrpSOS"+noOfVar+" / withinGroupMeanSqrError"+noOfVar+";\n";
        ecl += "OUTPUT("+getName()+",named('"+getName()+"'));";
        
        return(ecl);
    }
}

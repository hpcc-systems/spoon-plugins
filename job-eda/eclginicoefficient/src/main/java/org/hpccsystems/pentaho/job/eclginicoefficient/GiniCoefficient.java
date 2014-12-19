package org.hpccsystems.pentaho.job.eclginicoefficient;
/*
* To change this template, choose Tools | Templates
* and open the template in the editor.
*/



/** 
 *
* @author Siddharth
*/
public class GiniCoefficient {
       

    private String output;
    private String datasetName;
    private String field;
    //Comma separated list of fieldNames. a "-" prefix to the field name will indicate descending order
    
    public String getField() {
              return field;
       }

       public void setField(String field) {
              this.field = field;
       }
    
    public GiniCoefficient() {
              // TODO Auto-generated constructor stub
       Storage.noOfVariables++;
       }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }


    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }
    public String ecl() {
       
       int noOfVar = Storage.noOfVariables;
        String ecl ="IMPORT ML;\n";
        
        String sortedDatasetName = "GiniCoeffSortDataset"+noOfVar; 
        ecl += sortedDatasetName +" := SORT("+this.getDatasetName()+",'"+this.field+"');\n";      
        
       String count = "COuntGiniValuesinDataset"+noOfVar;
       ecl += count+" := COUNT("+sortedDatasetName+");\n";
       String NBRec = "NBRec"+noOfVar;
       ecl += NBRec+" := RECORD\nUNSIGNED id;\nREAL8 lowerterm;\nREAL8 upperterm;\nEND;\n";
       
       String idTrans = "idTrans"+noOfVar;
       ecl += NBRec+" "+idTrans+"("+sortedDatasetName+" L, INTEGER C,INTEGER lengthofdataset) := TRANSFORM\n   " +
                     "SELF.id := C;\n     " +
                     "SELF.lowerterm := L." +this.field+";\n"+
                     "SELF.upperterm := L."+this.field+"*(lengthofdataset+1-C);\n" +
                     "END;\n";
       String newGiniDatasetName = "newGiniDatasetName"+noOfVar;
       ecl += newGiniDatasetName +" := PROJECT("+sortedDatasetName+","+idTrans+"(LEFT,COUNTER,"+count+"));\n";    
       String mlVariable = "MLVariableGiniCOeff"+noOfVar;
       ecl += "ML.ToField("+newGiniDatasetName+","+mlVariable+");\n";
       
       
       
       
        String lowerVarName = "indVarName"+noOfVar;
        String upperVarName = "depVarName"+noOfVar;
        
        ecl += lowerVarName+" := "+mlVariable+"(NUMBER=1);\n";
        ecl += upperVarName+" := "+mlVariable+"(NUMBER=2);\n";
        
        
        String lowerAvgGini = "lowerAvgGini"+noOfVar;
        String upperAvgGini = "upperAvgGini"+noOfVar;
        
        
        ecl += lowerAvgGini +" := ave("+lowerVarName+",value);\n";
        ecl += upperAvgGini +" := ave("+upperVarName+",value);\n";
        
        String GiniTerm1 = "GiniTerm1"+noOfVar;
        ecl += GiniTerm1+" := 2*("+upperAvgGini+"/"+lowerAvgGini+");\n";
        
        String GiniTerm2 = "GiniTerm2"+noOfVar;
        ecl += GiniTerm2+" := "+count+" + 1 -"+GiniTerm1+";\n";
        
        ecl += this.getOutput() +" := "+GiniTerm2+"/"+count+";\n";
        ecl += "OUTPUT("+this.getOutput()+",named('"+this.getOutput()+"'));\n";
        
        return ecl;

    }
}

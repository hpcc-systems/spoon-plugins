/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ChambeJX
 */

import org.hpccsystems.ecldirect.ECLSoap;
import java.net.*;
import java.io.*;
import java.util.*;

import org.hpccsystems.ecldirect.Column;

public class testResultList {
    
    public static void main(String[] args) {
        String wuid = "W20120223-143628";
          String inFile = "spoon-ecl\\spoon-eclCode.ecl";
        String outFile = "spoon-ecl\\spoon-eclOut.ecl";
        String installDir = "C:\\Program Files\\HPCC Systems\\HPCC\\bin\\ver_3_0\\";
         //write ecl to file
        
        String inFilePath = "\"" + installDir + inFile + "\"";
        String outFilePath = "\"" + installDir + outFile + "\"";
        
        
        ECLSoap es = new ECLSoap();
        
        boolean isComplete = es.isComplete(wuid);
        System.out.println("isComplete: " + isComplete);
        InputStream is = es.InfoDetailsCall(wuid);
        try{
            ArrayList al = es.parseResultList(is);
             int n = al.size();
             System.out.println(al.toString());
             //rows
            for(int i = 0; i < n ; i++){
                System.out.println("-");
                ArrayList al2 = (ArrayList)al.get(i);
                int l = al2.size();
                //columns
                for(int k = 0; k < l ; k++){
                    System.out.println("--");
                 
                     ArrayList al3 = (ArrayList)al2.get(k);
                    int m = al3.size();
                    for(int j = 0; j < m ; j++){
                        
                        //if(((Column)al3.get(j)).getName().equals("Name")){
                            System.out.println("---");
                            String v2 = ((Column)al3.get(j)).getValue();
                            System.out.println("v2: " + v2);
                            InputStream outis = es.ResultsSoapCall(wuid,v2);
                            
                            ArrayList results = es.parseResults(outis);
                            //ystem.out.println("Size: " +results);
                            //ystem.out.println(outXml);
                        //}
                    }
                    
                }
            }
    
        }catch (Exception e){
         System.out.println(e.toString());   
        }
        
    }
    
}

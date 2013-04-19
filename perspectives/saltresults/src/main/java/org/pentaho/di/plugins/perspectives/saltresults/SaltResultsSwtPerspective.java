package org.pentaho.di.plugins.perspectives.saltresults;


import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Label;
import org.pentaho.di.core.EngineMetaInterface;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPerspectiveListener;
import org.pentaho.ui.xul.XulOverlay;
import org.pentaho.ui.xul.impl.DefaultXulOverlay;
import org.pentaho.ui.xul.impl.XulEventHandler;

//import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;
import org.eclipse.swt.widgets.Composite;

import java.io.*;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;


import org.eclipse.swt.*;
import org.eclipse.swt.layout.*;
import org.eclipse.swt.widgets.*;

import java.util.HashMap;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;

import com.lexisnexis.ui.clustercounts.ClusterCountsTable;
import com.lexisnexis.ui.datahygiene.DataHygieneTable;
import com.lexisnexis.ui.dataprofiling.DataProfileTable;
import com.lexisnexis.ui.datasummary.DataSummaryTable;
import com.lexisnexis.ui.optimizedlayout.OptimizedLayoutMain;
import com.lexisnexis.ui.sourceoutliers.SourceOutliersTable;


import au.com.bytecode.opencsv.CSVReader;



public class SaltResultsSwtPerspective implements SpoonPerspective {
  private Composite comp;
  private static SaltResultsSwtPerspective instance = new SaltResultsSwtPerspective();
  
  private String dataIn;
  private String fileName;
  private int fileCount;

  

  private int middle = 100;
  private int margin = 25;

  private Label lbl;
  private Shell parentShell;
  
  private CTabFolder folder;
  
  private boolean isActive;

  public void setFileName(String fn){
      fileName = fn;
      lbl.setText("Preview data returned from HPCC " + fileName);

  }
  public String getFileName(){
      return fileName;
  }
  
  private SaltResultsSwtPerspective(){  
    System.out.println("create saltResults SaltResultsSwtPerspective");
    createUI();
  }

  private void createUI(){
    String newFile = "";
    
    System.out.println("create saltResults GUI");
    //fileName = System.getProperty("fileName");
   // fileName =  System.getProperties().getProperty("fileName");
   // System.out.println("fileName" + fileName);
    
    parentShell = ((Spoon) SpoonFactory.getInstance()).getShell();
    
    Display display = parentShell.getDisplay();
   
    //dpt.createContents(parentShell);
    
    comp = new Composite(((Spoon) SpoonFactory.getInstance()).getShell(), SWT.BORDER);
    comp.setLayout(new GridLayout());
    comp.setLayoutData(new GridData(GridData.FILL_BOTH));
    
    Composite btnComp = new Composite(comp,SWT.NONE);
    GridLayout btnLayout = new GridLayout();
    btnLayout.numColumns = 2;
    btnComp.setLayout(btnLayout);
    lbl = new Label(comp, SWT.CENTER | SWT.TOP);
   
    Label lbl = new Label(comp, SWT.CENTER | SWT.TOP);
    GridData ldata = new GridData(SWT.CENTER, SWT.TOP, true, false);
    lbl.setLayoutData(ldata);
    lbl.setText("Preview data returned from SALt");
    
    Button fileButton = new Button(btnComp, SWT.PUSH | SWT.SINGLE | SWT.TOP);
    
    fileButton.setLayoutData(new GridData(SWT.LEFT, SWT.TOP, true, false));
    fileButton.setText("OPEN Data Profile FILE");
    
    
        
    //Listener for the file open button (fileButton)
    Listener fileOpenListener = new Listener() {

        public void handleEvent(Event e) {
            String newFile = buildFileDialog();
            if(newFile != ""){
                fileName = newFile;
                //TODO: create new tab for file
                //openFile(fileName);
                buildProfileTab(fileName);
                
                //int len = folder.getChildren().length;
                int len = folder.getItemCount();
                System.out.println("Number of tabs: " + len);
                folder.setSelection(len-1);
            }
        }
    };
     
    fileButton.addListener(SWT.Selection, fileOpenListener);
    Button fileButtonSummary = new Button(btnComp, SWT.PUSH | SWT.SINGLE | SWT.TOP);
    
    fileButtonSummary.setLayoutData(new GridData(SWT.LEFT, SWT.TOP, true, false));
    fileButtonSummary.setText("OPEN Data Summary FILE");
    
    
        
    //Listener for the file open button (fileButton)
    Listener fileSummaryOpenListener = new Listener() {

        public void handleEvent(Event e) {
            String newFile = buildFileDialog();
            if(newFile != ""){
                fileName = newFile;
                //TODO: create new tab for file
                //openFile(fileName);
                buildSummaryTab(fileName);
                
                //int len = folder.getChildren().length;
                int len = folder.getItemCount();
                System.out.println("Number of tabs: " + len);
                folder.setSelection(len-1);
            }
        }
    };
     
    fileButtonSummary.addListener(SWT.Selection, fileSummaryOpenListener);
    
   
    
    folder = new CTabFolder(comp, SWT.CLOSE);
    folder.setSimple(false);
    folder.setBorderVisible(true);
    folder.setLayoutData(new GridData(GridData.FILL_BOTH));
    
 
  }
  
  public void buildOptimizedLayoutTab(String thefilename){
	  OptimizedLayoutMain olm = new OptimizedLayoutMain(thefilename);
	  CTabItem tabOL = new CTabItem(folder,SWT.NONE);
	  tabOL.setText("Optimized Layout");
	  Text t = olm.createContents(folder);
	  tabOL.setControl(t);
  }
  
  public void buildSummaryTab(String thefilename){
	  System.out.println("Build Summary Tab: " + thefilename);
	    CTabItem tabSumm = new CTabItem(folder, SWT.NONE);
	    tabSumm.setText("Data Summary");
	    DataSummaryTable dst = new DataSummaryTable(thefilename);
	    Composite prof = dst.createContents(folder);
	    tabSumm.setControl(prof);
  }
  
  public void buildProfileTab(String filename){
            
	   
	    CTabItem tabDP = new CTabItem(folder, SWT.NONE);
	    
	    tabDP.setText("Data Profile");
	    DataProfileTable dpt = new DataProfileTable(filename);
	    dpt.setFileName(filename);
	    Composite prof = dpt.createContents(folder);
	    tabDP.setControl(prof);
	    
	    folder.setSelection(folder.getItemCount()-1);
  }

  public static SaltResultsSwtPerspective getInstance(){
        System.out.println("SaltResultsSwtPerspective");
    return instance;
  }

  public void setActive(boolean b) {
      //the function is called 3 times when leaving this perspective this odd
      //logic is here so it doesn't reload the data when leaving this perspective
      if(b){
    	  //each function verifies if there is data
    	  openProfiles();
    	  openSummaries();
    	  openOptimizedLayouts();
    	  openSrcOutliers();
    	  openClusterSrc();
    	  openClusterCounts();
    	  openSrcProfiles();
    	  openHygieneValidityErrors();
    	 // openCleanedData();
      }else{
           System.out.println("create saltResults setActive -- deactivate");
      }
  }
  
//SrcOutliers
  public void openSrcOutliers(){
	  ArrayList<String> files = parseData("saltSrcOutliers");
	  for(int i =0; i< files.size(); i++){
		  buildSrcOutliersTab(files.get(i));
	  }
  }
  
  public void buildSrcOutliersTab(String thefilename){

	    SourceOutliersTable table = new SourceOutliersTable(thefilename);
	    table.createContents(folder);
	   
  }
  //ClusterSrc
  public void openClusterSrc(){
	  ArrayList<String> files = parseData("saltClusterSrc");
	  for(int i =0; i< files.size(); i++){
		  buildClusterSrcTab(files.get(i));
	  }
  }
  public void buildClusterSrcTab(String thefilename){

	   // ClusterSourceTable table = new ClusterSrcTable(thefilename);
	   // table.createContents(folder);
	    
}
  
  //ClusterCounts
  public void openClusterCounts(){
	  ArrayList<String> files = parseData("saltClusterCounts");
	  for(int i =0; i< files.size(); i++){
		  buildClusterCountsTab(files.get(i));
	  }
  }
  public void buildClusterCountsTab(String thefilename){

	    ClusterCountsTable table = new ClusterCountsTable(thefilename);
	    table.createContents(folder);
	    
}
 
  //SrcProfiles
  public void openSrcProfiles(){
	  ArrayList<String> files = parseData("saltSrcProfiles");
	  for(int i =0; i< files.size(); i++){
		  buildSrcProfilesTab(files.get(i));
	  }
  }
  public void buildSrcProfilesTab(String thefilename){

	    //SourceOutliersTable table = new SourceOutliersTable(thefilename);
	    //table.createContents(folder);
	    
  }
  
  //Hygiene_ValidityErrors
  public void openHygieneValidityErrors(){
	  ArrayList<String> files = parseData("saltHygiene_ValidityErrors");
	  for(int i =0; i< files.size(); i++){
		  buildHygieneValidityErrorsTab(files.get(i));
	  }
  }
  public void buildHygieneValidityErrorsTab(String thefilename){
	    
	    DataHygieneTable table = new DataHygieneTable(thefilename);
	    table.createContents(folder);
	    
  }
 
  //CleanedData
  /*
  public void openCleanedData(){
	  ArrayList<String> files = parseData("saltCleanedData");
	  for(int i =0; i< files.size(); i++){
		  buildOptimizedLayoutTab(files.get(i));
	  }
  }
  public void buildCleanedDataTab(String thefilename){
	    CTabItem tab = new CTabItem(folder, SWT.NONE);
	    tab.setText("Src Outliers");
	    SourceOutliersTable table = new SourceOutliersTable(thefilename);
	    Composite prof = table.createContents(folder);
	    tab.setControl(prof);
  }*/
 
  
  public void openSummaries(){
	  ArrayList<String> files = parseData("saltSummaryData");
	  for(int i =0; i< files.size(); i++){
		  buildSummaryTab(files.get(i));
	  }
  }
  public void openOptimizedLayouts(){
	  ArrayList<String> files = parseData("saltOptimizedLayouts");
	  for(int i =0; i< files.size(); i++){
		  buildOptimizedLayoutTab(files.get(i));
	  }
  }
  public void openProfiles(){
	  ArrayList<String> files = parseData("saltData");
	  for(int i =0; i< files.size(); i++){
		  buildProfileTab(files.get(i));
	  }
  }
  public ArrayList parseData(String propName){
	  ArrayList<String> files = new ArrayList();
	  String saltData = "";
	  	 try{
	  		 if(System.getProperty(propName) != null){
	  			 saltData = System.getProperty(propName);
	  		 }
	  		StringTokenizer fileTokens = new StringTokenizer(saltData,",");
		     	while (fileTokens.hasMoreElements()) {
		     		String file = fileTokens.nextToken();
		     		
		     		if(file != null && !file.equals("null") && !file.equals("")){
		     			System.out.println("Built tab from list" + file);
		     			
		     			files.add(file);
		     		}
		     	}
		     	 saltData = "";
	  	 }catch (Exception e){
	  		 System.out.println("Failed to open files ");
	  	 }
	  	 //just incase it was a signle node
	  	 if(saltData != null && !saltData.equals("null") && !saltData.equals("")){
	  		System.out.println("Built tab from single " + saltData);
	  		files.add(saltData);
	  	 }
	  	System.setProperty(propName,"");
	  	return files;
  }

  
  public List<XulOverlay> getOverlays() {
    return Collections.singletonList((XulOverlay) new DefaultXulOverlay("org/pentaho/di/plugins/perspectives/saltresults/res/spoon_perspective_overlay.xul"));
  }

  public List<XulEventHandler> getEventHandlers() {
    return Collections.singletonList((XulEventHandler) new SaltResultsPerspectiveHandler());
  }

  public void addPerspectiveListener(SpoonPerspectiveListener spoonPerspectiveListener) {
       System.out.println("addPerspectiveListner");
  }

  public String getId() {
    //System.out.println("getId");
    return "saltresults";
  }

  
  // Whatever you pass out will be reparented. Don't construct the UI in this method as it may be called more than once.
  public Composite getUI() {
       //System.out.println("getUI");
    return comp;
  }

  public String getDisplayName(Locale locale) {
       //System.out.println("getDisplayName");
    return "Salt Results";
  }

  public InputStream getPerspectiveIcon() {
    ClassLoader loader = getClass().getClassLoader();
    return loader.getResourceAsStream("org/pentaho/di/plugins/perspectives/saltresults/res/blueprint.png");
  }

  /**
   * This perspective is not Document based, therefore there is no EngineMeta to save/open.
   * @return
   */
  public EngineMetaInterface getActiveMeta() {
       System.out.println("getActiveMeta");
    return null;
  }
  
  private void openFile(String fileName, Table table){
      String outStr;
      try{
    	  
    	  CSVReader reader = new CSVReader(new FileReader(fileName),',','"');
          String [] strLineArr;
          
          // Open the file that is the first 
          // command line parameter
          FileInputStream fstream = new FileInputStream(fileName);
          // Get the object of DataInputStream
          DataInputStream in = new DataInputStream(fstream);
          BufferedReader br = new BufferedReader(new InputStreamReader(in));
          String strLine;

          table.clearAll();
          table.removeAll();
          table.redraw();
          
          
          int length = 0;
          boolean first = true;
          //Read File Line By Line
          while ((strLineArr = reader.readNext()) != null) {
          //while ((strLine = br.readLine()) != null)   {
          // Print the content on the console
              //outStr = strLine;
              TableItem item = null;
              
              
              //item.setText(0,strLine);
              
              //StringTokenizer st = new StringTokenizer(strLine,",");
              //length = st.countTokens();
              //String[] strLineArr = strLine.split("\\,");
              if(first){
                  length = strLineArr.length;
              }else{
                   item = new TableItem (table, SWT.NONE);
              }
              
              /*
               * if(first){
                for (int i=0; i<length; i++) {
                    TableColumn column = new TableColumn (table, SWT.NONE);
                    column.setText (i + " col");
                }
              }else{
                  
              }*/
              int thisLen = strLineArr.length;
              if(thisLen<=length){
                  //String[] line = new String[length];
                  for(int i =0; i<thisLen; i++){
                      //line[i] = st.nextToken();
                      //item.setText (i, st.nextToken());
                      if(first){
                          TableColumn column = new TableColumn (table, SWT.NONE);
                          column.setText(strLineArr[i]);
                           //System.out.println("*");
                                    //System.out.println("**");
                                    //System.out.println("***");
                                    //System.out.println("****");
                                    //System.out.println("*****");
                                   // System.out.println(strLineArr[i]);
                      }else{
                         
                          //System.out.println("-- "+i+" -- " + strLineArr[i]);
                          item.setText(i,strLineArr[i]);
                      }
                  }
              }
              first = false;
              
          }
          
          //System.out.println("Finisehd file");
         
          for (int i=0; i<length; i++) {
		table.getColumn (i).pack ();
	  }
          //System.out.println("finished packing");
          //Close the input stream
          in.close();
    }catch (Exception e){//Catch exception if any
        System.err.println("Error: " + e.getMessage());
        e.printStackTrace();
     }
  
      
      
  }
  
  private String buildFileDialog() {
    
        //file field
        FileDialog fd = new FileDialog(parentShell, SWT.SAVE);

        fd.setText("Open");
        fd.setFilterPath("C:/");
        String[] filterExt = { "*.csv", "*.*" };
        fd.setFilterExtensions(filterExt);
        String selected = fd.open();
        if(!(fd.getFileName()).equalsIgnoreCase("")){
            return fd.getFilterPath() + System.getProperty("file.separator") + fd.getFileName();
        }else{
            return "";
        }
        
    }
  
  
  public void openSummaries_old(){
	  String saltData = "";
  	 try{
  		 if(System.getProperty("saltSummaryData") != null){
  			 saltData = System.getProperty("saltSummaryData");
  		 }
  		StringTokenizer fileTokens = new StringTokenizer(saltData,",");
	     	while (fileTokens.hasMoreElements()) {
	     		String file = fileTokens.nextToken();
	     		
	     		if(file != null && !file.equals("null") && !file.equals("")){
	     			System.out.println("Summary Built tab from list" + file);
	     			buildSummaryTab(file);
	     		}
	     	}
	     	 saltData = "";
  	 }catch (Exception e){
  		 System.out.println("Failed to open files ");
  	 }
  	 //just incase it was a signle node
  	 if(saltData != null && !saltData.equals("null") && !saltData.equals("")){
  		System.out.println("Summary Built tab from single " + saltData);
  		 buildSummaryTab(saltData);
  	 }
  	System.setProperty("saltSummaryData","");
  }
  
  public void openOptimizedLayouts_old(){
	  String saltData = "";
  	 try{
  		 if(System.getProperty("saltOptimizedLayouts") != null){
  			 saltData = System.getProperty("saltOptimizedLayouts");
  		 }
  		StringTokenizer fileTokens = new StringTokenizer(saltData,",");
	     	while (fileTokens.hasMoreElements()) {
	     		String file = fileTokens.nextToken();
	     		
	     		if(file != null && !file.equals("null") && !file.equals("")){
	     			System.out.println("OptimizedLayouts Built tab from list" + file);
	     			buildOptimizedLayoutTab(file);
	     		}
	     	}
	     	 saltData = "";
  	 }catch (Exception e){
  		 System.out.println("Failed to open files ");
  	 }
  	 //just incase it was a signle node
  	 if(saltData != null && !saltData.equals("null") && !saltData.equals("")){
  		System.out.println("OL Built tab from single " + saltData);
  		 buildSummaryTab(saltData);
  	 }
  	System.setProperty("saltOptimizedLayouts","");
  }
  
  public void openProfiles_old(){
	  String saltData = "";
  	 try{
  		 if(System.getProperty("saltData") != null){
  			 saltData = System.getProperty("saltData");
  		 }
  		StringTokenizer fileTokens = new StringTokenizer(saltData,",");
	     	while (fileTokens.hasMoreElements()) {
	     		String file = fileTokens.nextToken();
	     		
	     		if(file != null && !file.equals("null") && !file.equals("")){
	     			System.out.println("Built tab from list" + file);
	     			buildProfileTab(file);
	     		}
	     	}
	     	 saltData = "";
  	 }catch (Exception e){
  		 System.out.println("Failed to open files ");
  	 }
  	 //just incase it was a signle node
  	 if(saltData != null && !saltData.equals("null") && !saltData.equals("")){
  		System.out.println("Built tab from single " + saltData);
  		 buildProfileTab(saltData);
  	 }
  	System.setProperty("saltData","");
  }
  
}

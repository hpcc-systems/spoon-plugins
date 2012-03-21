package org.pentaho.di.plugins.perspectives.eclresults;

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


public class ECLResultsSwtPerspective implements SpoonPerspective {
  private Composite comp;
  private static ECLResultsSwtPerspective instance = new ECLResultsSwtPerspective();
  
  private String dataIn;
  private String fileName;
  private int fileCount;

  

  private int middle = 100;
  private int margin = 25;

  private Label lbl;
  private Shell parentShell;
  private Table table;
  
  private CTabFolder folder;
  
  private boolean isActive;

  public void setFileName(String fn){
      fileName = fn;
      lbl.setText("Preview data returned from HPCC " + fileName);

  }
  public String getFileName(){
      return fileName;
  }
  
  private ECLResultsSwtPerspective(){  
    System.out.println("create eclResults ECLResultsSwtPerspective");
    createUI();
  }

  private void createUI(){
    String newFile = "";
    
    System.out.println("create eclResults GUI");
    //fileName = System.getProperty("fileName");
   // fileName =  System.getProperties().getProperty("fileName");
   // System.out.println("fileName" + fileName);
    
    parentShell = ((Spoon) SpoonFactory.getInstance()).getShell();
    
    Display display = parentShell.getDisplay();
   
    comp = new Composite(((Spoon) SpoonFactory.getInstance()).getShell(), SWT.BORDER);
    comp.setLayout(new GridLayout());
    comp.setLayoutData(new GridData(GridData.FILL_BOTH));

    lbl = new Label(comp, SWT.CENTER | SWT.TOP);
   
    GridData ldata = new GridData(SWT.CENTER, SWT.TOP, true, false);
    lbl.setLayoutData(ldata);
    lbl.setText("Preview data returned from HPCC");

    Button fileButton = new Button(comp, SWT.PUSH | SWT.SINGLE | SWT.TOP);
 
    fileButton.setLayoutData(new GridData(SWT.LEFT, SWT.TOP, true, false));
    fileButton.setText("OPEN FILE");
    
    
        
    //Listener for the file open button (fileButton)
    Listener fileOpenListener = new Listener() {

        public void handleEvent(Event e) {
            String newFile = buildFileDialog();
            if(newFile != ""){
                fileName = newFile;
                //TODO: create new tab for file
                //openFile(fileName);
                buildTab(fileName);
                
                //int len = folder.getChildren().length;
                int len = folder.getItemCount();
                System.out.println("Number of tabs: " + len);
                folder.setSelection(len-1);
            }
        }
    };
     
  fileButton.addListener(SWT.Selection, fileOpenListener);
  
  
  //folder = new TabFolder(comp, SWT.CLOSE);
  
  folder = new CTabFolder(comp, SWT.CLOSE);
  folder.setSimple(false);
  folder.setBorderVisible(true);
  folder.setLayoutData(new GridData(GridData.FILL_BOTH));
  //folder.addCTabFolder2Listener(null);
   //for(int i = 0; i<5; i++){
 
	    //Tab 2
//	   buildTab("tab"+i);
       
          // openFile(fileName);     
  //}         
            
   /*     
  table = new Table (group, SWT.VIRTUAL | SWT.BORDER);
  table.setLinesVisible (true);
  table.setHeaderVisible (true);
  table.clearAll();
  GridData data = new GridData(SWT.FILL, SWT.FILL, true, true);
	data.heightHint = 200;
	table.setLayoutData(data);
  
 
       
   openFile(fileName);     
        
 */
     
  }
  
  
  public void buildTab(String filename){
            
    //TabItem tab2 = new TabItem(folder, SWT.NONE);
    CTabItem tab2 = new CTabItem(folder, SWT.NONE);
    tab2.setText(filename);
    
    Table table = new Table (folder, SWT.VIRTUAL | SWT.BORDER);
    table.setLinesVisible (true);
    table.setHeaderVisible (true);
    table.clearAll();
    GridData data = new GridData(SWT.FILL, SWT.FILL, true, true);
    data.heightHint = 200;
    table.setLayoutData(data);
    
    openFile(fileName,table);
    
    tab2.setControl(table);
  }

  public static ECLResultsSwtPerspective getInstance(){
        System.out.println("ECLResultsSwtPerspective");
    return instance;
  }

  public void setActive(boolean b) {
      //the function is called 3 times when leaving this perspective this odd
      //logic is here so it doesn't reload the data when leaving this perspective
      if(b){
         if(!this.isActive){
            System.out.println("create eclResults setActive");
            
            //openFile(fileName);
            
            //rebuild Tabs
            try{
                fileCount =  Integer.parseInt(System.getProperties().getProperty("fileCount"));
                System.setProperty("fileCount", "0");
            }catch (Exception e){
                fileCount = 0;
            }
            for(int i = 0; i<fileCount; i++){
                try{
                    fileName =  System.getProperties().getProperty("fileName"+i);
                    buildTab(fileName);
                    
                    //remove name from memory
                   
                    
                }catch (Exception e){
                    System.out.println("Failed to find and open file fileName" + i );
                }
            }
            //folder.setSelection(0);
            //int len = folder.getChildren().length;
            int len = folder.getItemCount();
            //System.out.println("Number of items: " + folder.getItemCount());
            System.out.println("Number of tabs: " + len);
            folder.setSelection(len-1);
            System.out.println("fileName" + fileName);
            System.out.println("setActive -- isactive = true");
            this.isActive = true;
         }else{
             this.isActive = false;
         }
          
      }else{
           System.out.println("create eclResults setActive -- deactivate");
          //  this.isActive = false;
      }
  }

  
  public List<XulOverlay> getOverlays() {
    return Collections.singletonList((XulOverlay) new DefaultXulOverlay("org/pentaho/di/plugins/perspectives/eclresults/res/spoon_perspective_overlay.xul"));
  }

  public List<XulEventHandler> getEventHandlers() {
    return Collections.singletonList((XulEventHandler) new ECLResultsPerspectiveHandler());
  }

  public void addPerspectiveListener(SpoonPerspectiveListener spoonPerspectiveListener) {
       System.out.println("addPerspectiveListner");
  }

  public String getId() {
    System.out.println("getId");
    return "eclresults";
  }

  
  // Whatever you pass out will be reparented. Don't construct the UI in this method as it may be called more than once.
  public Composite getUI() {
       System.out.println("getUI");
    return comp;
  }

  public String getDisplayName(Locale locale) {
       System.out.println("getDisplayName");
    return "ECL Results";
  }

  public InputStream getPerspectiveIcon() {
    ClassLoader loader = getClass().getClassLoader();
    return loader.getResourceAsStream("org/pentaho/di/plugins/perspectives/eclresults/res/blueprint.png");
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
          while ((strLine = br.readLine()) != null)   {
          // Print the content on the console
              outStr = strLine;
              TableItem item = null;
              
              
              //item.setText(0,strLine);
              
              //StringTokenizer st = new StringTokenizer(strLine,",");
              //length = st.countTokens();
              String[] strLineArr = strLine.split("\\,");
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
                         
                          System.out.println("-- "+i+" -- " + strLineArr[i]);
                          item.setText(i,strLineArr[i]);
                      }
                  }
              }
              first = false;
              
          }
          
          System.out.println("Finisehd file");
         
          for (int i=0; i<length; i++) {
		table.getColumn (i).pack ();
	  }
          System.out.println("finished packing");
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
  
  
  
  
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclpluginception;

import java.io.*;
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.IOException;
//import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.xml.sax.SAXException;

import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.ecljobentrybase.*;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
/**
 *
 * @author KeshavS
 */
public class ECLPluginceptionDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

	
    private ECLPluginception jobEntry;
    private Text jobEntryName;
    private Text PluginName;
    private Text plugins_dir;
    private Text t;
    private Combo datasetName;
    private Button wOK, wCancel;
    private boolean backupChanged;
    private Shell shellDialog;
    ArrayList<String> Items;
    ArrayList<String> Types;
    @SuppressWarnings("unused")
	private SelectionAdapter lsDef;
    private ArrayList<String> code;
    
    
    public ECLPluginceptionDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLPluginception) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("A plugin to create Plugins");
        }
    }

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        final Display display = parentShell.getDisplay();
        
        String datasets[] = null;
                
        AutoPopulate ap = new AutoPopulate();
        try{
            //Object[] jec = this.jobMeta.getJobCopies().toArray();
        	datasets = ap.parseDatasetsRecordsets(this.jobMeta.getJobCopies());
        }catch (Exception e){
            System.out.println("Error Parsing existing Datasets");
            System.out.println(e.toString());
            datasets = new String[]{""};
        }

        
        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        Items = new ArrayList<String>();
        Types = new ArrayList<String>();
        
        TabFolder tab = new TabFolder(shell, SWT.FILL | SWT.RESIZE | SWT.MIN | SWT.MAX);
        FormData datatab = new FormData();
        
        datatab.height = 250;
        datatab.width = 650;
        tab.setLayoutData(datatab);
        
        Composite compForGrp = new Composite(tab, SWT.NONE);
        //compForGrp.setLayout(new FillLayout(SWT.VERTICAL));
        compForGrp.setBackground(new Color(tab.getDisplay(),255,255,255));
        compForGrp.setLayout(new FormLayout());
        TabItem item1 = new TabItem(tab, SWT.NULL);
        
        item1.setText ("General");
        props.setLook(shell);
        JobDialog.setShellImage(shell, jobEntry);
        
        ModifyListener lsMod = new ModifyListener() {

            public void modifyText(ModifyEvent e) {
                jobEntry.setChanged();
            }
        };

        backupChanged = jobEntry.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;


        shell.setLayout(formLayout);
        shell.setText("MyECLCode");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Plugin to create Plugins");

        FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;


        // Stepname line
        Group generalGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(generalGroup);
        generalGroup.setText("General Details");
        generalGroup.setLayout(groupLayout);
        FormData generalGroupFormat = new FormData();
        generalGroupFormat.top = new FormAttachment(0, margin);
        generalGroupFormat.width = 340;
        generalGroupFormat.height = 65;
        generalGroupFormat.left = new FormAttachment(middle, 0);
        generalGroupFormat.right = new FormAttachment(100, 0);
        generalGroup.setLayoutData(generalGroupFormat);
		
		jobEntryName = buildText("Job Name :", null, lsMod, middle, margin, generalGroup);
		
		//All other contols
        //Dataset Declaration
        Group datasetGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(datasetGroup);
        datasetGroup.setText("Dataset Details");
        datasetGroup.setLayout(groupLayout);
        FormData datasetGroupFormat = new FormData();
        datasetGroupFormat.top = new FormAttachment(generalGroup, margin);
        datasetGroupFormat.width = 340;
        datasetGroupFormat.height = 120;
        datasetGroupFormat.left = new FormAttachment(middle, 0);
        datasetGroupFormat.right = new FormAttachment(100, 0);
        datasetGroup.setLayoutData(datasetGroupFormat);
		
		
        datasetName = buildCombo("Original Dataset :", jobEntryName, lsMod, middle, margin, datasetGroup, datasets);
        
        PluginName = buildText("Plugin Name:", datasetName, lsMod, middle, margin, datasetGroup);
        
        plugins_dir = buildText("Plugin Dir:", PluginName, lsMod, middle, margin, datasetGroup);
        
        item1.setControl(compForGrp);
        /**
         * Code Editor in new Tab starts
         */
        
        TabItem item2 = new TabItem(tab, SWT.NULL);
        item2.setText("Fields Selected");
		
        ScrolledComposite sc2 = new ScrolledComposite(tab, SWT.H_SCROLL | SWT.V_SCROLL);
        Composite compForGrp2 = new Composite(sc2, SWT.NONE);       
        compForGrp2.setLayout(new GridLayout(4, false));        
        sc2.setContent(compForGrp2);

        // Set the minimum size
        sc2.setSize(200, 250);

        // Expand both horizontally and vertically
        sc2.setExpandHorizontal(true);
        sc2.setExpandVertical(true);
        item2.setControl(sc2);
        
        
        final Table tbl = new Table(compForGrp2, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL); 
        GridData gd = new GridData(GridData.FILL_VERTICAL);
        gd.horizontalSpan = 1;
        tbl.setLayoutData(gd);
        tbl.setHeaderVisible(true);
        TableColumn tc = new TableColumn(tbl, SWT.NONE);
        tc.setWidth(150);
        tc.setText("Field(s) Dataset");
        tc = new TableColumn(tbl, SWT.NONE);
        tc.setWidth(80);
        tc.setText("Type");
        
        
        t = new Text(compForGrp2, SWT.MULTI | SWT.BORDER | SWT.WRAP | SWT.V_SCROLL | SWT.H_SCROLL);
        gd = new GridData(GridData.FILL_BOTH);
        gd.horizontalSpan = 3;
        t.setLayoutData(gd);
        
        Button Btn = new Button(compForGrp2, SWT.PUSH);
        Btn.setText("Add Column(s)");
        
        datasetName.addModifyListener(new ModifyListener(){

			@Override
			public void modifyText(ModifyEvent arg0) {
				tbl.setItemCount(0);				
			}
        	
        });
        
        
        
        
        
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, tab);
        
        Btn.addSelectionListener(new SelectionListener (){
			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				AutoPopulate ap = new AutoPopulate();
		    	RecordList rec = null;
		    	String[] items = null;
		    	String[] types = null;
		    	 
		    	if(!datasetName.getText().equals("")){
	            	  
	                  //RecordList rec = ap.rawFieldsByDataset( datasetName.getText(),jobMeta.getJobCopies());
	                  
	                  
	            	  try {
	            		  items = ap.fieldsRecByDataset( datasetName.getText(),jobMeta.getJobCopies());
						//items = ap.fieldsByDataset( datasetName.getText(),jobMeta.getJobCopies());
					} catch (Exception e) {
						
						e.printStackTrace();
					}
	            	  rec = ap.buildMyRecordList(items);
	                  
	                  
	            	  int i = 0;types = new String[items.length];
	            	  for(Iterator<RecordBO> I = rec.getRecords().iterator();I.hasNext();){
							RecordBO ob = (RecordBO) I.next();							
							types[i] = ob.getColumnType()+ob.getColumnWidth();
							i++;
							
						}	            	  
	              }
				pouplateTable(tbl,items,types);
				if(items != null){
					for(int i = 0; i<items.length; i++){
						Items.add(items[i].split(",")[0]);
						Types.add(types[i]);
					}
				}
			}
        	
        });
        
        
        // Add listeners
        Listener cancelListener = new Listener() {

            public void handleEvent(Event e) {
                cancel();
            }
        };
        Listener okListener = new Listener() {

            public void handleEvent(Event e) {
            	code = new ArrayList<String>();
            	String[] S = t.getText().split("\n");
            	for(int i = 0; i<S.length; i++){
            		code.add(S[i]);
            	}
                ok(display,code);
            }
        };

        wCancel.addListener(SWT.Selection, cancelListener);
        wOK.addListener(SWT.Selection, okListener);

        lsDef = new SelectionAdapter() {

            public void widgetDefaultSelected(SelectionEvent e) {            	
                ok(display,code);
            }
        };


        // Detect X or ALT-F4 or something that kills this window...

        shell.addShellListener(new ShellAdapter() {

            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        if (jobEntry.getName() != null) {
            jobEntryName.setText(jobEntry.getName());
        }
        
        if (jobEntry.getDatasetName() != null) {
            datasetName.setText(jobEntry.getDatasetName());
        }
        
        if (jobEntry.getresultDatasetName() != null) {
        	PluginName.setText(jobEntry.getresultDatasetName());
        }
             
        if (jobEntry.getText() != null) {
            t.setText(jobEntry.getText());
        }

        if (jobEntry.getDir() != null) {
            plugins_dir.setText(jobEntry.getDir());
        }
        
        if (jobEntry.getItems() != null || jobEntry.getTypes() != null) {
            Items = jobEntry.getItems();
            Types = jobEntry.getTypes();
            String[] items = new String[Items.size()];
            String[] types = new String[Types.size()];
            int i = 0;
            for(Iterator<String> it = Items.iterator(); it.hasNext();){            
            	items[i] = it.next();
            	i++;
            }
            i = 0;
            for(Iterator<String> it = Types.iterator(); it.hasNext();){            
            	types[i] = it.next();
            	i++;
            }
            pouplateTable(tbl,items,types);
        }
              
        shell.pack();
        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        if(shell.isDisposed()){
        	shellDialog.dispose();
        }

        return jobEntry;

    }
    
    private void pouplateTable(Table tbl, String[] items, String[] types){
    	if(items != null){
    		for(int i = 0; i<items.length; i++){
    			TableItem it = new TableItem(tbl, SWT.NONE);
    			it.setText(0, items[i].split(",")[0]);
    			if(types.length > 0)
    				it.setText(1, types[i]);
    			else
    				it.setText(1, "");
    	}
    	}
    }

    private boolean validate(){
    	boolean isValid = true;
    	String errors = "";
    	
    	
    	
    	if(this.jobEntryName.getText().equals("")){
    		isValid = false;
    		errors += "\"Job Entry Name\" is a required field!\r\n";
    	}
    	
    	
    	
    	if(this.plugins_dir.getText().equals("")){
    		isValid = false;
    		errors += "Path to \"Spoon Plugins Directory\" is a required field!\r\n";
    	}
    	
    	if(!isValid){
    		ErrorNotices en = new ErrorNotices();
    		errors += "\r\n";
    		errors += "If you continue to save with errors you may encounter compile errors if you try to execute the job.\r\n\r\n";
    		isValid = en.openValidateDialog(getParent(),errors);
    	}
    	return isValid;
    	
    }
    
    private void ok(Display display, ArrayList<String> code) {
    	if(!validate()){
    		return;
    	}
    	
        jobEntry.setName(jobEntryName.getText());
        jobEntry.setDatasetName(datasetName.getText());      
        jobEntry.setresultDatasetName(this.PluginName.getText());
        jobEntry.setText(this.t.getText());
        jobEntry.setItems(Items);
        jobEntry.setTypes(Types);
        jobEntry.setDir(plugins_dir.getText());
        try {
        				
        	create_plugin(code,display,plugins_dir.getText());
		
        } catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerFactoryConfigurationError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        shellDialog.dispose();
        dispose();
    }
    
    private void create_plugin(ArrayList<String> code, Display display, String path) throws TransformerFactoryConfigurationError, Exception{ 
    	
    	// path is the path to your spoon plugins folder where the spoon jobs files reside
    	//path = path.replaceAll("\\", "\\"+"\\"); 
    	
    	File src = new File(path+"\\job-eda\\pluginhelper");//D:\\Users\\703119704\\Documents\\spoon-plugins\\spoon-plugins
    	File dest = new File(path+"\\job-eda\\ecl"+PluginName.getText().toLowerCase());//D:\\Users\\703119704\\Documents\\spoon-plugins\\spoon-plugins
    	CopyFolder obj = new CopyFolder();
    	obj.copy(src, dest);
    	
    	
    	Change chn = new Change();
    	    	
    	chn.change_project(PluginName,path);
    	chn.write_pom(PluginName,path);
    	
    	CodeFactory cf = new CodeFactory();
    	cf.writeCodeModel(code,PluginName.getText(),path); 
    	
    	chn.change_plugin(PluginName,path);
    	
    	shellDialog = new Shell(display);
    	//String PluginName = "pluginception";
    	shellDialog.setLayout(new GridLayout(1,false));
    	shellDialog.setBackground(new Color(null,255,255,255));
    	shellDialog.setSize(400, 400);     
    	shellDialog.setText("Compile Dialog");
    	
    	Text box = new Text(shellDialog, SWT.NONE | SWT.V_SCROLL | SWT.WRAP);
    	
    	box.setLayoutData(new GridData(GridData.FILL_BOTH));
    	box.setText("Compiling...");
    	shellDialog.open();
        
        
        ProcessBuilder builder = new ProcessBuilder("cmd.exe", "/c", "cd \"job-eda\\ecl"+PluginName.getText().toLowerCase()+"\" && mvn -s ..\\..\\settings.xml install");
        builder = builder.directory(new File(path));
        //spoon-plugins\\spoon-plugins\\job\\ecl"+PluginName+"
        //mvn -s ..\\..\\settings.xml install
        builder.redirectErrorStream(true);
        Process p = builder.start();
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        while (true) {
            line = r.readLine();            
            if (line == null) { break; }
            box.append(line+"\n");
        } 
        
        
        while (!shellDialog.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
    	//display.dispose();
    }
    

}

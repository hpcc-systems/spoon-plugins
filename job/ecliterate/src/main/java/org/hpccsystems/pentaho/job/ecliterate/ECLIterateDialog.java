/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecliterate;

import java.util.Iterator;

import java.util.Map;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
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
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.Text;
import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.recordlayout.CreateTable;
import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.mapper.MainMapper;
import org.hpccsystems.mapper.MapperRecordList;
import org.hpccsystems.mapper.Utils;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.hpccsystems.mapper.*;
import org.hpccsystems.ecljobentrybase.*;

/**
 *
 * @author ChalaAX
 */
public class ECLIterateDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

    private ECLIterate jobEntry;
    
    private Text jobEntryName;
    private Text transformName;
    private Combo runLocal;
    private Combo dataset;
    private Text recordsetName;
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
    
  //mapper specific
    private CreateTable tblOutput = null;
    private MainMapper tblMapper = null;
    private MapperRecordList mapperRecList = new MapperRecordList();
    //end mapper specific 

    public ECLIterateDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLIterate) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Iterate");
        }
    }
    
    
    public void buildGeneralTab(Composite compForGrp, FormLayout groupLayout,Group generalGroup, ModifyListener lsMod, int middle, int margin){
    	
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
        
        
   	//All other contols
       //Project Declaration
       Group iterateGroup = new Group(compForGrp, SWT.SHADOW_NONE);
       props.setLook(iterateGroup);
       iterateGroup.setText("Rollup Details");
       iterateGroup.setLayout(groupLayout);
       
       FormData groupFormat = new FormData();
       groupFormat.top = new FormAttachment(generalGroup, 5);
       groupFormat.bottom = new FormAttachment(wOK, -5);
       groupFormat.left = new FormAttachment(generalGroup, 0, SWT.LEFT);
       groupFormat.right = new FormAttachment(generalGroup, 0, SWT.RIGHT);
       iterateGroup.setLayoutData(groupFormat);
       
       FormData data = new FormData(50, 25);
		data.right = new FormAttachment(50, 0);
		data.bottom = new FormAttachment(100, 0);
		wOK.setLayoutData(data);
		
		data = new FormData(50, 25);
		data.left = new FormAttachment(wOK, 5);
		data.bottom = new FormAttachment(wOK, 0, SWT.BOTTOM);
		wCancel.setLayoutData(data);

		recordsetName = buildText("Resulting Recordset", null, lsMod, middle, margin, iterateGroup); 
		dataset = buildCombo("In Recordset Name", recordsetName, lsMod, middle, margin, iterateGroup,datasets);//input record
		transformName = buildText("Transform Name", dataset, lsMod, middle, margin, iterateGroup);
		runLocal = buildCombo("RUNLOCAL", transformName, lsMod, middle, margin, iterateGroup, new String[]{"false", "true"});
    
      // parameterName = buildText("Parameter Name", transformName, lsMod, middle, margin, distributeGroup);
       
       //transformFormat = buildMultiText("Transform Format", parameterName, lsMod, middle, margin, distributeGroup);
       
		
       
   }

    public JobEntryInterface open() {
    	 AutoPopulate ap = new AutoPopulate();
        Shell parentShell = getParent();
        Display display = parentShell.getDisplay();

        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

        tblOutput = new CreateTable(shell); //Instantiate the Table to be used in "Output Format" tab

        
        props.setLook(shell);
        JobDialog.setShellImage(shell, jobEntry);
        ModifyListener lsMod = new ModifyListener() {

            public void modifyText(ModifyEvent e) {
                jobEntry.setChanged();
            }
        };

        backupChanged = jobEntry.hasChanged();

        FormLayout formLayout = new FormLayout();
        //formLayout.marginWidth = Const.FORM_MARGIN; //5
        //formLayout.marginHeight = Const.FORM_MARGIN; //5
        
        shell.setLayout(formLayout);
        shell.setSize(800,600); //800 X 600 (width X Height)

        int middle = props.getMiddlePct(); //35. This value is defined in org.pentaho.di.core.Const.
        int margin = Const.MARGIN; //4. This value is defined in org.pentaho.di.core.Const.

        shell.setLayout(formLayout);
        shell.setText("Define an ECL Iterate");
        
        //Create a Tab folder and add an event which gets the updated recordlist and populates the Variable Name drop down. 
        final TabFolder tabFolder = new TabFolder (shell, SWT.FILL | SWT.RESIZE | SWT.MIN | SWT.MAX);
        tabFolder.addSelectionListener(new SelectionAdapter() {
        	public void widgetSelected(org.eclipse.swt.events.SelectionEvent event) {
        		if(tabFolder.getSelectionIndex() == 2){
        			if(tblOutput.getRecordList() != null && tblOutput.getRecordList().getRecords().size() > 0) {
        				String[] cmbValues = new String[tblOutput.getRecordList().getRecords().size()];
        				int count = 0;
        				cmbValues[0] = "SELF";
        				for (Iterator<RecordBO> iterator = tblOutput.getRecordList().getRecords().iterator(); iterator.hasNext();) {
        					RecordBO obj = (RecordBO) iterator.next();
        					cmbValues[count] = "self." + obj.getColumnName();
        					count++;
        				}
                      
        				tblMapper.getCmbVariableName().removeAll();
        				tblMapper.getCmbVariableName().setItems(cmbValues);
        			}
        		}
            }
        });

        //Start of code for Tabs

        FormData tabFolderData = new FormData();
        tabFolderData.height = 500;
        tabFolderData.width = 700;
        tabFolderData.top = new FormAttachment(0, 0);
        tabFolderData.left = new FormAttachment(0, 0);
        tabFolderData.right = new FormAttachment(100, 0);
        tabFolderData.bottom = new FormAttachment(100, 0);
        tabFolder.setLayoutData(tabFolderData);
        
        //Tab Item 1 for "General" Tab
        //CTabItem item1 = new CTabItem(tabFolder, SWT.NONE);
        TabItem item1 = new TabItem(tabFolder, SWT.NULL);
        ScrolledComposite sc = new ScrolledComposite(tabFolder, SWT.H_SCROLL | SWT.V_SCROLL);
        Composite compForGrp = new Composite(sc, SWT.NONE);
        compForGrp.setLayout(new FormLayout());
        
        compForGrp.setBackground(new Color(sc.getDisplay(),255,255,255));
        
        sc.setContent(compForGrp);

        // Set the minimum size
        sc.setMinSize(650, 450);

        // Expand both horizontally and vertically
        sc.setExpandHorizontal(true);
        sc.setExpandVertical(true);
        
        item1.setText ("General");
        item1.setControl(sc);
        
        //Define buttons since they are used for component alignment 
        wOK = new Button(compForGrp, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(compForGrp, SWT.PUSH);
        wCancel.setText("Cancel");
        
        FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;

        // Stepname line
        Group generalGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(generalGroup);
        generalGroup.setText("General Details");
        generalGroup.setLayout(groupLayout);
        
        FormData generalGroupFormat = new FormData();
        generalGroupFormat.height = 65;
        generalGroupFormat.left = new FormAttachment(0, 5);
        generalGroupFormat.right = new FormAttachment(100, -5);
        generalGroup.setLayoutData(generalGroupFormat);
        
        jobEntryName = buildText("Job Entry Name", null, lsMod, middle, margin, generalGroup);

        buildGeneralTab(compForGrp,groupLayout, generalGroup,lsMod, middle, margin);

		//Tab Item 3 for "Transform Format" Tab
        //CTabItem item3 = new CTabItem(tabFolder, SWT.NONE);
		TabItem item3 = new TabItem(tabFolder, SWT.NULL);
		
		ScrolledComposite sc3 = new ScrolledComposite(tabFolder, SWT.H_SCROLL | SWT.V_SCROLL);
		final Composite compForGrp3 = new Composite(sc3, SWT.NONE);
		sc3.setContent(compForGrp3);

		// Set the minimum size
		sc3.setMinSize(650, 450);

		// Expand both horizontally and vertically
		sc3.setExpandHorizontal(true);
		sc3.setExpandVertical(true);
        
		item3.setText ("Transform Format");
        item3.setControl(sc3);
		
        GridLayout mapperCompLayout = new GridLayout();
        mapperCompLayout.numColumns = 1;
        GridData mapperCompData = new GridData();
		mapperCompData.grabExcessHorizontalSpace = true;
		compForGrp3.setLayout(mapperCompLayout);
		compForGrp3.setLayoutData(mapperCompData);
		
		Map<String, String[]> mapDataSets = null;
		try {
			mapDataSets = ap.parseDefExpressionBuilder(this.jobMeta.getJobCopies());
		} catch(Exception e) {
			e.printStackTrace();
		}
		 
		//Create a Mapper
		String[] cmbValues = {"SELF"};
		tblMapper = new MainMapper(compForGrp3, mapDataSets, cmbValues);
		
		//Add the existing Mapper RecordList
        if(jobEntry.getMapperRecList() != null){
        	mapperRecList = jobEntry.getMapperRecList();
            tblMapper.setMapperRecList(jobEntry.getMapperRecList());
        }
		
        tblMapper.reDrawTable();
        
        // Add listeners
        Listener cancelListener = new Listener() {

            public void handleEvent(Event e) {
                cancel();
            }
        };
        Listener okListener = new Listener() {

            public void handleEvent(Event e) {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, cancelListener);
        wOK.addListener(SWT.Selection, okListener);

        lsDef = new SelectionAdapter() {

            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };


        // Detect X or ALT-F4 or something that kills this window...

        shell.addShellListener(new ShellAdapter() {

            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });
        
        this.dataset.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
                   System.out.print("inRecordName Listner");
		        try{
		        	populateDatasets();
		        }catch (Exception excep){
		        	System.out.println("Failed to load datasets");
		        }
            };
        });



        if (jobEntry.getName() != null) {
            jobEntryName.setText(jobEntry.getName());
        }

        
        //load fields
        
        this.dataset.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
		        try{
		        	populateDatasets();
		        }catch (Exception excep){
		        	System.out.println("Failed to load datasets");
		        }
            };
        });
		
       	dataset.setText(jobEntry.getDataset());
           try{
           	populateDatasets();
           }catch (Exception e){
           	System.out.println("Failed to load datasets");
           }
       if (jobEntry.getRecordsetName() != null) {//output
           recordsetName.setText(jobEntry.getRecordsetName());
       }  

       if (jobEntry.getTransformName() != null) {
           transformName.setText(jobEntry.getTransformName());
       }
       
       if (jobEntry.getRunLocalString() != null) {
           runLocal.setText(jobEntry.getRunLocalString());
       }
       
       if (jobEntry.getDataset() != null) {
           dataset.setText(jobEntry.getDataset());
       }
        
        
        

        //shell.pack();
        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return jobEntry;

    }
    
    
    private void populateDatasets() throws Exception{
    	if(dataset.getText().equals("")){
    		return;
    	}
    	
    	AutoPopulate ap = new AutoPopulate();
    	Map<String, String[]> mapDataSets = null;
		try {
			mapDataSets = ap.parseDefExpressionBuilder(this.jobMeta.getJobCopies(), dataset.getText());
		} catch(Exception e) {
			e.printStackTrace();
		}
		//(tblMapper.getTreeInputDataSet()).clearAll(false);
		(tblMapper.getTreeInputDataSet()).removeAll();
		//Utils.fillTree(tblMapper.getTreeInputDataSet(), mapDataSets);
		Utils.fillTree("LEFT", "L",tblMapper.getTreeInputDataSet(), mapDataSets);
		Utils.fillTree("RIGHT", "R",tblMapper.getTreeInputDataSet(), mapDataSets);
		Utils.fillTreeVariableName(tblMapper, tblMapper.getTreeInputDataSet(), mapDataSets);
		//System.out.println("Updated Input Recordsets");
        tblMapper.reDrawTable();
    }
    
    
    
  
    
    

    private boolean validate(){
    	boolean isValid = true;
    	String errors = "";
    	
    	//only need to require a entry name
    	if(this.jobEntryName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Job Entry Name\"!\r\n";
    	}
    	
    	if(this.recordsetName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Resulting Recordset\"!\r\n";
    	}
    	
    	if(this.dataset.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"In Recordset Name\"!\r\n";
    	}
    	
    	if(this.transformName.getText().equals("")){
    		//one is required.
    		isValid = false;
    		errors += "You must provide a \"Transform Name\"!\r\n";
    	}
    	
    	//TODO: update this doesn't seem to work.
    	if(tblMapper.getMapperRecList().equals("")){
    		isValid = false;
    		errors += "You must provide a \"Transform Format\"!\r\n";
    	}

		if(!isValid){
			ErrorNotices en = new ErrorNotices();
			errors += "\r\n";
			errors += "If you continue to save with errors you may encounter compile errors if you try to execute the job.\r\n\r\n";
			isValid = en.openValidateDialog(getParent(),errors);
		}
		return isValid;
		
	}
    
    private void ok() {
    	if(!validate()){
    		return;
    	}
    	
        jobEntry.setName(jobEntryName.getText());
        jobEntry.setTransformName(transformName.getText());
        jobEntry.setRecordsetName(recordsetName.getText());
        jobEntry.setRunLocalString(runLocal.getText());
        jobEntry.setDataset(dataset.getText());
        jobEntry.setMapperRecList(tblMapper.getMapperRecList());

        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

   
}

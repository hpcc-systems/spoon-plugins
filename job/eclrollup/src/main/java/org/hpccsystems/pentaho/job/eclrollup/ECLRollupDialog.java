/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclrollup;




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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.Text;
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


import org.hpccsystems.eclguifeatures.*;
import org.hpccsystems.recordlayout.*;
import org.hpccsystems.ecljobentrybase.*;


/**
 *
 * @author ChambersJ
 */
public class ECLRollupDialog extends ECLJobEntryDialog{
//public class ECLRollupDialog extends ECLJobEntryDialog {

    private ECLRollup jobEntry;
    
    private Text jobEntryName;

    private Text recordsetName;
    private Combo dataset;
   // private Text recordFormat;
    private Text condition;
    private Text transformName;
    private Text transform;
    private Text transformCall;
    private Text fieldlist;
    private Combo group;
    private Combo runLocal;//optional
    
    //mapper specific
    private CreateTable tblOutput = null;
    private MainMapper tblMapper = null;
    //private RecordList recordList = new RecordList();
    private MapperRecordList mapperRecList = new MapperRecordList();
    //end mapper specific 
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
    
    
    
    

    public boolean isBackupChanged() {
        return backupChanged;
    }

    public void setBackupChanged(boolean backupChanged) {
        this.backupChanged = backupChanged;
    }

    public Text getCondition() {
        return condition;
    }

    public void setCondition(Text condition) {
        this.condition = condition;
    }

    public Text getFieldlist() {
        return fieldlist;
    }

    public void setFieldlist(Text fieldlist) {
        this.fieldlist = fieldlist;
    }

    public Combo getGroup() {
        return group;
    }

    public void setGroup(Combo group) {
        this.group = group;
    }

    public ECLRollup getJobEntry() {
        return jobEntry;
    }

    public void setJobEntry(ECLRollup jobEntry) {
        this.jobEntry = jobEntry;
    }

    public Text getJobEntryName() {
        return jobEntryName;
    }

    public void setJobEntryName(Text jobEntryName) {
        this.jobEntryName = jobEntryName;
    }

    public SelectionAdapter getLsDef() {
        return lsDef;
    }

    public void setLsDef(SelectionAdapter lsDef) {
        this.lsDef = lsDef;
    }

    public Combo getRecordset() {
        return dataset;
    }

    public void setRecordset(Combo recordset) {
        this.dataset = recordset;
    }

    public Combo getRunLocal() {
        return runLocal;
    }

    public void setRunLocal(Combo runLocal) {
        this.runLocal = runLocal;
    }

    public Text getTransform() {
        return transform;
    }

    public void setTransform(Text transform) {
        this.transform = transform;
    }

    public Text getTransformCall() {
        return transformCall;
    }

    public void setTransformCall(Text transformCall) {
        this.transformCall = transformCall;
    }

    public Text getTransformName() {
        return transformName;
    }

    public void setTransformName(Text transformName) {
        this.transformName = transformName;
    }

    public Button getwCancel() {
        return wCancel;
    }

    public void setwCancel(Button wCancel) {
        this.wCancel = wCancel;
    }

    public Button getwOK() {
        return wOK;
    }

    public void setwOK(Button wOK) {
        this.wOK = wOK;
    }

    public ECLRollupDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLRollup) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Rollup");
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
        //recordFormat = buildText("Record Definition", dataset, lsMod, middle, margin, iterateGroup);//input recordDef
        
        transformName = buildText("Transform Name", dataset, lsMod, middle, margin, iterateGroup);
        
        //transform = buildMultiText("Transform", transformName, lsMod, middle, margin, iterateGroup);
        condition = buildText("Condtion", transformName, lsMod, middle, margin, iterateGroup);
        fieldlist = buildMultiText("Fieldlist", condition, lsMod, middle, margin, iterateGroup);
        group = buildCombo("Grouped", fieldlist, lsMod, middle, margin, iterateGroup,new String[]{"no", "yes"});
        
        //transformCall = buildMultiText("Transform Call", group, lsMod, middle, margin, iterateGroup);
        runLocal = buildCombo("RUNLOCAL", group, lsMod, middle, margin, iterateGroup, new String[]{"false", "true"});
     
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
        shell.setText("Define an ECL Rollup");
        
        //Create a Tab folder and add an event which gets the updated recordlist and populates the Variable Name drop down. 
        final TabFolder tabFolder = new TabFolder (shell, SWT.FILL | SWT.RESIZE | SWT.MIN | SWT.MAX);
        tabFolder.addSelectionListener(new SelectionAdapter() {
        	public void widgetSelected(org.eclipse.swt.events.SelectionEvent event) {
        		if(tabFolder.getSelectionIndex() == 2){
        			if(tblOutput.getRecordList() != null && tblOutput.getRecordList().getRecords().size() > 0) {
        				String[] cmbValues = new String[tblOutput.getRecordList().getRecords().size()];
        				int count = 1;
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
                   System.out.print("inRecordName Listner");
		        try{
		        	populateDatasets();
		        }catch (Exception excep){
		        	System.out.println("Failed to load datasets");
		        }
            };
        });
        
        if (jobEntry.getRecordset() != null) {
        	dataset.setText(jobEntry.getRecordset());
            try{
            	populateDatasets();
            }catch (Exception e){
            	System.out.println("Failed to load datasets");
            }
        }
        if (jobEntry.getRecordsetName() != null) {
            recordsetName.setText(jobEntry.getRecordsetName());
        }
        
        if (jobEntry.getCondition() != null) {
            condition.setText(jobEntry.getCondition());
        }
        
        if (jobEntry.getTransformName() != null) {
            transformName.setText(jobEntry.getTransformName());
        }
        
        if (jobEntry.getFieldlist() != null) {
            fieldlist.setText(jobEntry.getFieldlist());
        }
       
        if (jobEntry.getGroup() != null) {
            group.setText(jobEntry.getGroup());
        }
        if (jobEntry.getRunLocalString() != null) {
            runLocal.setText(jobEntry.getRunLocalString());
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
		Utils.fillTree("LEFT", "L",tblMapper.getTreeInputDataSet(), mapDataSets);
		Utils.fillTree("RIGHT", "R",tblMapper.getTreeInputDataSet(), mapDataSets);
		
		
		Utils.fillTreeVariableName(tblMapper, tblMapper.getTreeInputDataSet(), mapDataSets);		
		
		
		
		//System.out.println("Updated Input Recordsets");
        tblMapper.reDrawTable();
    }
    

    
    public Button buildButton(String strLabel, Control prevControl, 
             ModifyListener isMod, int middle, int margin, Composite groupBox){
        
            Button nButton = new Button(groupBox, SWT.PUSH | SWT.SINGLE | SWT.CENTER);
            nButton.setText(strLabel);
            props.setLook(nButton);
            //nButton.addModifyListener(lsMod)
            FormData fieldFormat = new FormData();
            
            fieldFormat.left = new FormAttachment(middle, 0);
            fieldFormat.top = new FormAttachment(prevControl, margin);
            fieldFormat.right = new FormAttachment(75, 0);
            fieldFormat.height = 25;

            nButton.setLayoutData(fieldFormat);
        
            return nButton;
            
           
    }
    public String buildFileDialog() {
        
            //file field
            FileDialog fd = new FileDialog(shell, SWT.SAVE);

            fd.setText("Save");
            fd.setFilterPath("C:/");
            String[] filterExt = { "*.csv", ".xml", "*.txt", "*.*" };
            fd.setFilterExtensions(filterExt);
            String selected = fd.open();
            if(fd.getFileName() != ""){
                return fd.getFilterPath() + System.getProperty("file.separator") + fd.getFileName();
            }else{
                return "";
            }
            
        }

    public Text buildText(String strLabel, Control prevControl,
                ModifyListener lsMod, int middle, int margin, Composite groupBox) {
            // label
            Label fmt = new Label(groupBox, SWT.RIGHT);
            fmt.setText(strLabel);
            props.setLook(fmt);
            FormData labelFormat = new FormData();
            labelFormat.left = new FormAttachment(0, 0);
            labelFormat.top = new FormAttachment(prevControl, margin);
            labelFormat.right = new FormAttachment(middle, -margin);
            fmt.setLayoutData(labelFormat);

            // text field
            Text text = new Text(groupBox, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
            props.setLook(text);
            text.addModifyListener(lsMod);
            FormData fieldFormat = new FormData();
            fieldFormat.left = new FormAttachment(middle, 0);
            fieldFormat.top = new FormAttachment(prevControl, margin);
            fieldFormat.right = new FormAttachment(100, 0);
            text.setLayoutData(fieldFormat);

            return text;
        }

    public Text buildMultiText(String strLabel, Control prevControl,
                ModifyListener lsMod, int middle, int margin, Composite groupBox) {
            // label
            Label fmt = new Label(groupBox, SWT.RIGHT);
            fmt.setText(strLabel);
            props.setLook(fmt);
            FormData labelFormat = new FormData();
            labelFormat.left = new FormAttachment(0, 0);
            labelFormat.top = new FormAttachment(prevControl, margin);
            labelFormat.right = new FormAttachment(middle, -margin);
            fmt.setLayoutData(labelFormat);

            // text field
            Text text = new Text(groupBox, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL);
            props.setLook(text);
            text.addModifyListener(lsMod);
            FormData fieldFormat = new FormData();
            fieldFormat.left = new FormAttachment(middle, 0);
            fieldFormat.top = new FormAttachment(prevControl, margin);
            fieldFormat.right = new FormAttachment(100, 0);
            fieldFormat.height = 100;
            text.setLayoutData(fieldFormat);

            return text;
        }

    public Combo buildCombo(String strLabel, Control prevControl,
                ModifyListener lsMod, int middle, int margin, Composite groupBox, String[] items) {
            // label
            Label fmt = new Label(groupBox, SWT.RIGHT);
            fmt.setText(strLabel);
            props.setLook(fmt);
            FormData labelFormat = new FormData();
            labelFormat.left = new FormAttachment(0, 0);
            labelFormat.top = new FormAttachment(prevControl, margin);
            labelFormat.right = new FormAttachment(middle, -margin);
            fmt.setLayoutData(labelFormat);

            // combo field
            Combo combo = new Combo(groupBox, SWT.MULTI | SWT.LEFT | SWT.BORDER);
            props.setLook(combo);
            combo.setItems(items);
            combo.addModifyListener(lsMod);
            FormData fieldFormat = new FormData();
            fieldFormat.left = new FormAttachment(middle, 0);
            fieldFormat.top = new FormAttachment(prevControl, margin);
            fieldFormat.right = new FormAttachment(100, 0);
            fieldFormat.height = 50;
            combo.setLayoutData(fieldFormat);

            return combo;
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
    	
    	if(this.condition.getText().equals("") && this.fieldlist.getText().equals("") && this.group.getText().equals("")){
    		//one must be set
    		isValid = false;
    		errors += "You must provide one of the following: \"Condition\", \"Fieldlist\", \"Group\"!\r\n";
    	}else{
    		//only one of the 3 should be allowed
    		boolean a = !this.condition.getText().equals("");//not empty string
    		boolean b = !this.fieldlist.getText().equals("");
    		boolean c = !(this.group.getText().equals("") || this.group.getText().equals("no"));
    		if(!((a && !b && !c) || (!a && b && !c) || (!a && !b && c))){
    			isValid = false;
    			errors += "You must provide only one of the following: \"Condition\", \"Fieldlist\", \"Group\"!\r\n";
    		}
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
        //jobEntry.setJobName(jobEntryName.getText());
        jobEntry.setName(jobEntryName.getText());
                                            /*
                private String recordset;
                private String condition;
                private String transform;
                private String fieldlist;
                private String GROUP;
                private Boolean runLocal;
             */
        jobEntry.setRecordsetName(recordsetName.getText());
        
        jobEntry.setRecordset(dataset.getText());
        jobEntry.setCondition(condition.getText());
        jobEntry.setTransformName(transformName.getText());
        jobEntry.setFieldlist(fieldlist.getText());
        jobEntry.setGroup(group.getText());
        jobEntry.setRunLocalString(runLocal.getText());
        //jobEntry.setRecordFormat(recordFormat.getText());
        
       // jobEntry.setRecordList(tblOutput.getRecordList());
        jobEntry.setMapperRecList(tblMapper.getMapperRecList());

        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }
}

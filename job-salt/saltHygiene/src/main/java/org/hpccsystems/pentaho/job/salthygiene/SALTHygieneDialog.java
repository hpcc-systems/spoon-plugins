/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.salthygiene;

import java.util.Iterator;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FillLayout;
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
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
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

import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.eclguifeatures.ErrorNotices;
//import org.hpccsystems.recordlayout.RecordBO;
//import org.hpccsystems.saltui.
import org.hpccsystems.saltui.hygiene.*;
import org.hpccsystems.ecljobentrybase.*;
/**
 *
 * @author ChambersJ
 */
public class SALTHygieneDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

    private SALTHygiene jobEntry;
    private Text jobEntryName;
    private Combo datasetName;
    private Combo layout;
    private Combo cleanedOutput;
    private Label outLabel;
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
    
    CreateTable createTable = null;
    private HygieneEntryList entryList;
    private HygieneRuleList fieldTypeList;
    
    private Combo srcField;
	private Combo includeSrcOutliers;
    private Combo includeClusterSrc;
    private Combo includeClusterCounts;
    private Combo includeSrcProfiles;


    public SALTHygieneDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (SALTHygiene) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Salt Hygiene");
        }
    }

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        Display display = parentShell.getDisplay();
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
        
        //see need to get the dataset set on screen a pass its fields in
        
        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        createTable = new CreateTable(shell);
        TabFolder tabFolder = new TabFolder (shell, SWT.FILL | SWT.RESIZE | SWT.MIN | SWT.MAX);
        FormData data = new FormData();
        
        data.height = 500;
        data.width = 650;
        tabFolder.setLayoutData(data);
        
        Composite compForGrp = new Composite(tabFolder, SWT.NONE);
        //compForGrp.setLayout(new FillLayout(SWT.VERTICAL));
        compForGrp.setBackground(new Color(tabFolder.getDisplay(),255,255,255));
        compForGrp.setLayout(new FormLayout());
        
        TabItem item1 = new TabItem(tabFolder, SWT.NULL);
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
        shell.setText("Salt Hygiene");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define Salt Hygiene Rules");

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
        generalGroupFormat.width = 400;
        generalGroupFormat.height = 65;
        generalGroupFormat.left = new FormAttachment(middle, 0);
        generalGroup.setLayoutData(generalGroupFormat);
        
        jobEntryName = buildText("Job Entry Name", null, lsMod, middle, margin, generalGroup);

        //All other contols
        //Join Declaration
        Group hygieneGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(hygieneGroup);
        hygieneGroup.setText("Data Profiling Details");
        hygieneGroup.setLayout(groupLayout);
        FormData hygieneGroupFormat = new FormData();
        hygieneGroupFormat.top = new FormAttachment(generalGroup, margin);
        hygieneGroupFormat.width = 400;
        hygieneGroupFormat.height = 150;
        hygieneGroupFormat.left = new FormAttachment(middle, 0);
        hygieneGroup.setLayoutData(hygieneGroupFormat);
        
        

        item1.setControl(compForGrp);
        this.datasetName = buildCombo("Dataset Name", null, lsMod, middle, margin, hygieneGroup,datasets);
        this.cleanedOutput = buildCombo("Output Cleaned Dataset?", this.datasetName, lsMod,middle,margin,hygieneGroup,new String[]{"yes","no"});
        //this.layout = buildCombo("Layout", this.datasetName, lsMod, middle, margin, joinGroup,datasets);
        outLabel = buildLabel("The Output will be stored as a file on the cluster as \r\n" +
        		"~SPOONFILES::[Dataset Name]_CleanedData.\r\n\r\n", this.cleanedOutput, lsMod, 0, margin, hygieneGroup);
        String[] items = null;
        try{
	        items = ap.fieldsByDataset( datasetName.getText(),jobMeta.getJobCopies()); 
        }catch (Exception exc){
        	System.out.println("error loading dataset fields");
        }

        Group optionalGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(optionalGroup);
        optionalGroup.setText("Data Source Cluster Diagnostic Reports");
        optionalGroup.setLayout(groupLayout);
        FormData optionalGroupFormat = new FormData();
        optionalGroupFormat.top = new FormAttachment(hygieneGroup, margin);
        optionalGroupFormat.width = 400;
        optionalGroupFormat.height = 150;
        optionalGroupFormat.left = new FormAttachment(middle, 0);
        optionalGroup.setLayoutData(optionalGroupFormat);
        
        this.srcField = buildCombo("Source Field", null, lsMod, middle, margin, optionalGroup,items);
        this.includeSrcOutliers = buildCombo("Include SrcOutliers", this.srcField, lsMod,middle,margin,optionalGroup,new String[]{"yes","no"});
        this.includeClusterSrc = buildCombo("Include ClusterSrc?", this.includeSrcOutliers, lsMod,middle,margin,optionalGroup,new String[]{"yes","no"});
        this.includeClusterCounts = buildCombo("Include Cluster Counts?", this.includeClusterSrc, lsMod,middle,margin,optionalGroup,new String[]{"yes","no"});
        this.includeSrcProfiles = buildCombo("Include SrcProfiles?", this.includeClusterCounts, lsMod,middle,margin,optionalGroup,new String[]{"yes","no"});

        
        createTable = new CreateTable(shell);
        try{
	       // String[] items = ap.fieldsByDataset( datasetName.getText(),jobMeta.getJobCopies());
	        createTable.loadFields(items);
	        
        }catch (Exception exc){
        	System.out.println("error loading dataset fields");
        }
        datasetName.addModifyListener(new ModifyListener(){
            public void modifyText(ModifyEvent e){
                System.out.println("left RS changed");
                AutoPopulate ap = new AutoPopulate();
                try{
                	String[] items = ap.fieldsByDataset( datasetName.getText(),jobMeta.getJobCopies());
                	populateSrcField(datasetName.getText(), items);
                	createTable.loadFields(items);
                	outLabel.setText("The Output will be stored as a file on the cluster as \r\n~SPOONFILES::" + datasetName.getText() + "_CleanedData.\r\n\r\n");
                }catch (Exception exc){
                	System.out.println("error loading dataset fields");
                }
            }
        });
        
        createTable = new CreateTable(shell);
        
        //String[] items = ap.fieldsByDataset( leftRecordSet.getText(),jobMeta.getJobCopies());
        
        if(jobEntry.getEntryList() != null){
            entryList = jobEntry.getEntryList();
            createTable.setEntryList(jobEntry.getEntryList());
            /*
            if(entryList.getEntries() != null && entryList.getEntries().size() > 0) {
                    System.out.println("Size: "+entryList.getEntries().size());
                    for (Iterator<EntryBO> iterator = entryList.getEntries().iterator(); iterator.hasNext();) {
                            EntryBO obj = (EntryBO) iterator.next();
                    }
            }*/
        }
        if(jobEntry.getFieldTypeList() != null){
        	fieldTypeList = jobEntry.getFieldTypeList();
        	createTable.setRuleList(jobEntry.getFieldTypeList());
        }
        TabItem item2 = createTable.buildDetailsTab("Rules", tabFolder);
       
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, tabFolder);

        
       
      
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

        tabFolder.setRedraw(true);
        // Detect X or ALT-F4 or something that kills this window...

        shell.addShellListener(new ShellAdapter() {

            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        if (jobEntry.getName() != null) {
            jobEntryName.setText(jobEntry.getName());
        }
       
        //if (jobEntry.getJoinCondition() != null) {
            //this.joinCondition.setText(jobEntry.getJoinCondition());
        //}
        if (jobEntry.getDatasetName() != null) {
            
           // AutoPopulate ap = new AutoPopulate();
            try{
            	String[] newItems = ap.fieldsByDataset( datasetName.getText(),jobMeta.getJobCopies());
            	populateSrcField(datasetName.getText(), newItems);
            }catch (Exception exc){
            	System.out.println("error loading dataset fields");
            }
            this.datasetName.setText(jobEntry.getDatasetName());
        }
       // if (jobEntry.getLayout() != null) {
        //    this.layout.setText(jobEntry.getLayout());
        //}
        if (jobEntry.getCleanData() != null) {
            this.cleanedOutput.setText(jobEntry.getCleanData());
        }
        
        
        if (jobEntry.getIncludeClusterCounts() != null) {
            this.includeClusterCounts.setText(jobEntry.getIncludeClusterCounts());
        }
        
        if (jobEntry.getIncludeClusterSrc() != null) {
            this.includeClusterSrc.setText(jobEntry.getIncludeClusterSrc());
        }
        
        if (jobEntry.getIncludeSrcOutliers() != null) {
            this.includeSrcOutliers.setText(jobEntry.getIncludeSrcOutliers());
        }
        if (jobEntry.getIncludeSrcProfiles() != null) {
            this.includeSrcProfiles.setText(jobEntry.getIncludeSrcProfiles());
        }
        if (jobEntry.getSrcField() != null) {
            this.srcField.setText(jobEntry.getSrcField());
        }


        shell.pack();
        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return jobEntry;

    }
    
    public void populateSrcField(String dsName, String[] items){
    	//this.srcField;
    	this.srcField.removeAll();
    	for (int i = 0; i<items.length; i++){
    		this.srcField.add(items[i]);

        }
    }
    
    private TabItem buildDetailsTab2(String tabName, TabFolder tabFolder){
    	
    	TabItem tabItem = new TabItem(tabFolder, SWT.NULL);
		tabItem.setText(tabName);
		
		/*Composite composite = new Composite(tabFolder, SWT.NONE);
		tabItem.setControl(composite);*/

		ScrolledComposite sc2 = new ScrolledComposite(tabFolder, SWT.H_SCROLL | SWT.V_SCROLL);
		Composite compForGrp2 = new Composite(sc2, SWT.NONE);
		sc2.setContent(compForGrp2);

		// Set the minimum size
		sc2.setMinSize(650, 450);

		// Expand both horizontally and vertically
		sc2.setExpandHorizontal(true);
		sc2.setExpandVertical(true);
        
		
	
		/*
		Composite compForGrp2 = new Composite(tabFolder, SWT.NONE);
        //compForGrp.setLayout(new FillLayout(SWT.VERTICAL));
        compForGrp2.setBackground(new Color(tabFolder.getDisplay(),255,255,255));
        compForGrp2.setLayout(new FormLayout());
        */
        
		//this.addChildControls(compForGrp2);
        FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;
        ModifyListener lsMod = new ModifyListener() {

            public void modifyText(ModifyEvent e) {
                jobEntry.setChanged();
            }
        };
        
		/*Group generalGroup2 = new Group(compForGrp2, SWT.SHADOW_NONE);
        props.setLook(generalGroup2);
        generalGroup2.setText("General Details");
        generalGroup2.setLayout(groupLayout);
        FormData generalGroupFormat2 = new FormData();
        generalGroupFormat2.top = new FormAttachment(0, 0);
        generalGroupFormat2.width = 400;
        generalGroupFormat2.height = 65;
        generalGroupFormat2.left = new FormAttachment(0, 0);
        generalGroup2.setLayoutData(generalGroupFormat2);
        
        Text jobEntryName2 = buildText("Job Entry Name", null, lsMod, 0, 0, generalGroup2);
        */
        GridData gridData = new GridData (GridData.HORIZONTAL_ALIGN_FILL | GridData.FILL_BOTH);
        compForGrp2.setLayoutData (gridData);

		// Set numColumns to 6 for the buttons 
		GridLayout layout = new GridLayout(6, false);
		compForGrp2.setLayout (layout);
		
		
		int style = SWT.CHECK | SWT.MULTI | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL | SWT.FULL_SELECTION;

		final Table table = new Table(compForGrp2, style);
		
		final TableViewer tableViewer = new TableViewer(table);
                
                table.addListener (SWT.Selection, new Listener () {
                    public void handleEvent (Event event) {
                            tableViewer.refresh();
                            table.redraw();
                    }
                });      
		GridData tableGridData = new GridData(GridData.FILL_BOTH);
		tableGridData.grabExcessVerticalSpace = true;
		tableGridData.horizontalSpan = 4;
		tableGridData.grabExcessHorizontalSpace = false;
		tableGridData.widthHint = 600;
		table.setLayoutData(tableGridData);		
					
		table.setLinesVisible(true);
		table.setHeaderVisible(true);
		
		TableColumn column = new TableColumn(table, SWT.LEFT, 0);
        column.setText("Fields");
        column.setWidth(250);
        
        TableColumn column2 = new TableColumn(table, SWT.LEFT, 1);
        column2.setText("Rules");
        column2.setWidth(250);
        
        TableColumn column3 = new TableColumn(table, SWT.CENTER, 2);
        column3.setText("");
        column3.setWidth(50);
        
        TableColumn column4 = new TableColumn(table, SWT.CENTER, 3);
        column4.setText("");
        column4.setWidth(50);

        //tabItem.setControl(compForGrp2);
        tabItem.setControl(sc2);
		return tabItem;
    }

    private boolean validate(){
    	boolean isValid = true;
    	String errors = "";
    	
    	
    	
    	if(this.jobEntryName.getText().equals("")){
    		isValid = false;
    		errors += "\"Job Entry Name\" is a required field!\r\n";
    	}
    	
    	if(this.datasetName.getText().equals("")){
    		isValid = false;
    		errors += "\"Dataset Name\" is a required field!\r\n";
    	}
    	//if(this.layout.getText().equals("")){
    	//	isValid = false;
    	//	errors += "\"Layout\" is a required field!\r\n";
    	//}
    	
    	
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
        jobEntry.setDatasetName(datasetName.getText());
        //jobEntry.setLayout(layout.getText());
        jobEntry.setEntryList(createTable.getEntryList());
        jobEntry.setFieldTypeList(createTable.getRuleList());
        jobEntry.setCleanData((this.cleanedOutput.getText()));
        
        jobEntry.setIncludeClusterCounts((this.includeClusterCounts.getText()));
        jobEntry.setIncludeClusterSrc((this.includeClusterSrc.getText()));
        jobEntry.setIncludeSrcOutliers((this.includeSrcOutliers.getText()));
        jobEntry.setIncludeSrcProfiles((this.includeSrcProfiles.getText()));
        jobEntry.setSrcField((this.srcField.getText()));
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

}

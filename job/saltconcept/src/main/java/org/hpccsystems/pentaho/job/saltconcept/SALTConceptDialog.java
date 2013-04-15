/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.saltconcept;

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
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.saltui.concept.*;
import org.hpccsystems.saltui.concept.edit.*;
import org.hpccsystems.ecljobentrybase.*;
/**
 *
 * @author ChambersJ
 */
public class SALTConceptDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

    private SALTConcept jobEntry;
    private Text jobEntryName;
    private Combo datasetName;
    private Combo layout;
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
    
    CreateTable createTable = null;
    private ConceptEntryList entryList;
    //private ConceptRuleList fieldTypeList;
    



    public SALTConceptDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (SALTConcept) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("Salt Concept");
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
        shell.setText("Salt Concept");

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define Salt Concept Fields");

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
        Group joinGroup = new Group(compForGrp, SWT.SHADOW_NONE);
        props.setLook(joinGroup);
        joinGroup.setText("Data Profiling Details");
        joinGroup.setLayout(groupLayout);
        FormData joinGroupFormat = new FormData();
        joinGroupFormat.top = new FormAttachment(generalGroup, margin);
        joinGroupFormat.width = 400;
        joinGroupFormat.height = 150;
        joinGroupFormat.left = new FormAttachment(middle, 0);
        joinGroup.setLayoutData(joinGroupFormat);

        item1.setControl(compForGrp);
        this.datasetName = buildCombo("Dataset Name", null, lsMod, middle, margin, joinGroup,datasets);
       

        createTable = new CreateTable(shell);
        String[] items = null;// = ap.fieldsByDataset( datasetName.getText(),jobMeta.getJobCopies());
        try{
	        //String[] items = ap.fieldsByDataset( datasetName.getText(),jobMeta.getJobCopies());
        	items = ap.fieldsByDataset( datasetName.getText(),jobMeta.getJobCopies());
	        createTable.loadFields(items);
	        
        }catch (Exception exc){
        	exc.printStackTrace();
        	System.out.println("error loading dataset fields");
        }
        datasetName.addModifyListener(new ModifyListener(){
            public void modifyText(ModifyEvent e){
                System.out.println("left RS changed");
                AutoPopulate ap = new AutoPopulate();
                try{
                	String[] items = ap.fieldsByDataset( datasetName.getText(),jobMeta.getJobCopies());
                	createTable.loadFields(items);
                	}catch (Exception exc){
                	System.out.println("error loading dataset fields");
                }
            }
        });
        
        createTable = new CreateTable(shell,items);
        
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
       // if(jobEntry.getFieldTypeList() != null){
       // 	fieldTypeList = jobEntry.getFieldTypeList();
        //	createTable.setRuleList(jobEntry.getFieldTypeList());
        //}
        TabItem item2 = createTable.buildDetailsTab("Concepts", tabFolder);
        
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
            this.datasetName.setText(jobEntry.getDatasetName());
        }
       
        tabFolder.redraw();
        shell.pack();
        shell.open();
        while (!shell.isDisposed()) {
        	//if(createTable.hasChanged){
        	//	createTable.refreshTable();
        	//}
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return jobEntry;

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
        
		
	
        FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;
        ModifyListener lsMod = new ModifyListener() {

            public void modifyText(ModifyEvent e) {
                jobEntry.setChanged();
            }
        };
        
		
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
        column.setText("Concepts");
        column.setWidth(500);
      
        
        TableColumn column3 = new TableColumn(table, SWT.CENTER, 1);
        column3.setText("");
        column3.setWidth(50);
        
        TableColumn column4 = new TableColumn(table, SWT.CENTER, 2);
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
      //  jobEntry.setFieldTypeList(createTable.getEntryList());
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

}

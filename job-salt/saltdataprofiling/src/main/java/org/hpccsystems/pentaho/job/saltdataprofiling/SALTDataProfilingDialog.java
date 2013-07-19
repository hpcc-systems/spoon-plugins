/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.saltdataprofiling;

import java.util.Iterator;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
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
import org.hpccsystems.recordlayout.CreateTable;
import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
import org.hpccsystems.ecljobentrybase.*;
/**
 *
 * @author ChambersJ
 */
public class SALTDataProfilingDialog extends ECLJobEntryDialog{//extends JobEntryDialog implements JobEntryDialogInterface {

    private SALTDataProfiling jobEntry;
    private Text jobEntryName;
    private Combo datasetName;
    private Combo layout;
    
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
    



    public SALTDataProfilingDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (SALTDataProfiling) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("SaltDataProfling");
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
        
        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

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
       
        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Salt Data Profiling");

        FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;

        // Stepname line
        Group generalGroup = new Group(shell, SWT.SHADOW_NONE);
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
        Group joinGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(joinGroup);
        joinGroup.setText("Data Profiling Details");
        joinGroup.setLayout(groupLayout);
        FormData joinGroupFormat = new FormData();
        joinGroupFormat.top = new FormAttachment(generalGroup, margin);
        joinGroupFormat.width = 400;
        joinGroupFormat.height = 150;
        joinGroupFormat.left = new FormAttachment(middle, 0);
        joinGroup.setLayoutData(joinGroupFormat);

        
        this.datasetName = buildCombo("Dataset Name", null, lsMod, middle, margin, joinGroup,datasets);
        //this.layout = buildCombo("Layout", this.datasetName, lsMod, middle, margin, joinGroup,datasets);
       
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, joinGroup);

        
        /*leftRecordSet.addModifyListener(new ModifyListener(){
            public void modifyText(ModifyEvent e){
                System.out.println("left RS changed");
                AutoPopulate ap = new AutoPopulate();
                try{
                    System.out.println("Load items for select");
                    String[] items = ap.fieldsByDataset( leftRecordSet.getText(),jobMeta.getJobCopies());
                    System.out.println("++++++"+items.length+"+++++");
                    leftJoinCondition.setItems(items);
                    System.out.println("itemsSet");
                    leftJoinCondition.redraw();
                    System.out.println("new Join condition vals loaded");
                }catch (Exception ex){
                    System.out.println("failed to load record definitions");
                    System.out.println(ex.toString());
                    ex.printStackTrace();
                }
               // leftJoinCondition.setItems(items);
            }
        });
      
        rightRecordSet.addModifyListener(new ModifyListener(){
            public void modifyText(ModifyEvent e){
                System.out.println("left RS changed");
                AutoPopulate ap = new AutoPopulate();
                try{
                    System.out.println("Load items for select");
                    String[] items = ap.fieldsByDataset( rightRecordSet.getText(),jobMeta.getJobCopies());
                    System.out.println("++++++"+items.length+"+++++");
                    rightJoinCondition.setItems(items);
                    System.out.println("itemsSet");
                    rightJoinCondition.redraw();
                    System.out.println("new Join condition vals loaded");
                }catch (Exception ex){
                    System.out.println("failed to load record definitions");
                    System.out.println(ex.toString());
                    ex.printStackTrace();
                }
               // leftJoinCondition.setItems(items);
            }
        });
          */
        //this.leftRecordSet.addListener(SWT.Selection, leftRecordSetListener);
        //this.leftRecordSet.addModifyListener(leftRecordSetListener);
      
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

        if (jobEntry.getName() != null) {
            jobEntryName.setText(jobEntry.getName());
        }
       
        //if (jobEntry.getJoinCondition() != null) {
            //this.joinCondition.setText(jobEntry.getJoinCondition());
        //}
        if (jobEntry.getDatasetName() != null) {
            this.datasetName.setText(jobEntry.getDatasetName());
        }
       // if (jobEntry.getLayout() != null) {
        //    this.layout.setText(jobEntry.getLayout());
        //}
        

        shell.pack();
        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return jobEntry;

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
       
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

}


package org.hpccsystems.pentaho.job.ecladdcount;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.ecljobentrybase.ECLJobEntryDialog;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;


/**
 * Generate User Defined Plugin
 *  @author KeshavS
 * 
 */
public class ECLAddCountDialog
    extends ECLJobEntryDialog
{

    private ECLAddCount jobEntry;
    private Text jobEntryName;
    private Combo inDS;
    private Text OutDS;
    private Button wOK;
    private Button wCancel;
    private boolean backupChanged;
    private ModifyEvent Me;
    private ModifyListener Ml;
    private Listener Li;
    private SelectionAdapter lsDef;
    private SelectionEvent Se;
    private ShellEvent SHe;
    private Event Ev;
    private ShellAdapter Sa;
    private SWT Swt;
    public int margin = Const.MARGIN;
    private FormAttachment FA;

    public ECLAddCountDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent,jobEntryInt,rep,jobMeta);
        jobEntry = (ECLAddCount) jobEntryInt;
        if (this.jobEntry.getName() == null) {
        	this.jobEntry.setName("AddCount");
        }
    }

    /**
     * Create Gooey
     * 
     */
    public JobEntryInterface open() {
        Shell parentShell = (getParent());
        Display display = (parentShell.getDisplay());
        String datasets[] = null;
        AutoPopulate ap = (new AutoPopulate());
        try{
        	datasets = ap.parseDatasetsRecordsets(this.jobMeta.getJobCopies());
        }
        catch (Exception e){
        	System.out.println("Error Parsing existing Datasets");
        	System.out.println(e.toString());
        	datasets = new String[]{""};
        }
        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        props.setLook(shell);
        JobDialog.setShellImage((shell), (jobEntry));
        ModifyListener lsMod = new ModifyListener(){
        	public void modifyText(ModifyEvent e) {
        		jobEntry.setChanged();
        	}
        };
        backupChanged = jobEntry.hasChanged();
        FormLayout formLayout = (new FormLayout());
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        

        int middle = props.getMiddlePct();
        shell.setLayout(formLayout);
        shell.setText("AddCount");
        FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;
        

        Group generalGroup = (new Group(shell, SWT.SHADOW_NONE));
        props.setLook(generalGroup);
        generalGroup.setText("General Details");
        generalGroup.setLayout(groupLayout);
        FormData generalGroupFormat = (new FormData());
        generalGroupFormat.top = new FormAttachment(0, margin);
        generalGroupFormat.width = 340;
        generalGroupFormat.height = 65;
        generalGroupFormat.left = new FormAttachment(middle, 0);
        generalGroupFormat.right = new FormAttachment(100, 0);
        generalGroup.setLayoutData(generalGroupFormat);
        

        

        Group datasetGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(datasetGroup);
        datasetGroup.setText("MyDetails");
        datasetGroup.setLayout(groupLayout);
        FormData datasetGroupFormat = new FormData();
        datasetGroupFormat.top = new FormAttachment(generalGroup, margin);
        datasetGroupFormat.width = 340;
        datasetGroupFormat.height = 120;
        datasetGroupFormat.left = new FormAttachment(middle, 0);
        datasetGroupFormat.right = new FormAttachment(100, 0);
        datasetGroup.setLayoutData(datasetGroupFormat);
        

        jobEntryName = buildText("Job Name :", null, lsMod, middle, margin, generalGroup);
        

        inDS = buildCombo("Dataset :", jobEntryName, lsMod, middle, margin, datasetGroup, datasets);
        OutDS = buildText("Result Dataset:", inDS, lsMod, middle, margin, datasetGroup);
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");
        

        BaseStepDialog.positionBottomButtons((shell), (new Button[]{wOK,wCancel}), (margin), (datasetGroup));
        

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
        

        shell.addShellListener(new ShellAdapter() {
        	public void shellClosed(ShellEvent e) {
        		cancel();
        	}
        });
        

        if (jobEntry.getName() != null) {
        	jobEntryName.setText(jobEntry.getName());
        }
        

        if (jobEntry.getinDS() != null) {
        	inDS.setText(jobEntry.getinDS());
        }
        

        if (jobEntry.getOutDS() != null) {
        	OutDS.setText(jobEntry.getOutDS());
        }
        

        shell.pack();
        shell.open();
        while (!shell.isDisposed()) {
        	if (!display.readAndDispatch()) {
        		display.sleep();
        	}
        }
        

        return (jobEntry);
    }

    /**
     * Create Validation Checks
     * 
     */
    private boolean validate() {
        boolean isValid = true;
        String errors = "";
        if(this.jobEntryName.getText().equals("")){
        	isValid = false;
        	errors += "\"Job Entry Name\" is a required field!\r\n";
        }
        

        if(this.inDS.getText().equals("")){
        	isValid = false;
        	errors += "\"inDS\" is a required field!\r\n";
        }
        

        if(this.OutDS.getText().equals("")){
        	isValid = false;
        	errors += "\"OutDS\" is a required field!\r\n";
        }
        

        if(!isValid){
        ErrorNotices en = (new ErrorNotices());
        errors += "\r\n";
        errors += "If you continue to save with errors you may encounter compile errors if you try to execute the job.\r\n\r\n";
        isValid = en.openValidateDialog(getParent(),errors);
        }
        return (isValid);
    }

    /**
     * Create OK_Button function
     * 
     */
    private void ok() {
        if(!validate()){
        	return;
        }
        jobEntry.setName(jobEntryName.getText());
        jobEntry.setinDS(inDS.getText());
        jobEntry.setOutDS(OutDS.getText());
        dispose();
    }

    /**
     * Create Cancel_Button function
     * 
     */
    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

}

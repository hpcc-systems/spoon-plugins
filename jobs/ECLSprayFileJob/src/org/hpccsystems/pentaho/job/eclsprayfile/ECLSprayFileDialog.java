/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.eclsprayfile;



import java.util.Iterator;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Label;

import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
//import org.eclipse.swt.layout.GridData;
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
import org.hpccsystems.eclguifeatures.RecordList;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;


import org.hpccsystems.eclguifeatures.CreateTable;
import org.hpccsystems.eclguifeatures.RecordBO;


/**
 *
 * @author ChalaAX
 */
public class ECLSprayFileDialog extends JobEntryDialog implements JobEntryDialogInterface {

    private ECLSprayFile jobEntry;
    private Text jobEntryName;
    //private Text ipAddress;
    private Text filePath;
    private Text logicalFileName;
    private Combo fileType;
    private Text csvSeperator;
    private Text csvQuote;
    private Text csvTerminator;
    private Text fixedRecordSize;
    private Button wOK, wCancel;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
    
    
    private Table table;
    private TableViewer tableViewer;

    // Create a RecordList and assign it to an instance variable
    private RecordList recordList = new RecordList();
    private CreateTable ct = null;


    // Set the table column property names
    private final String CHECKED_COLUMN 		= "checked";
    private final String NAME_COLUMN 			= "column_name";
    private final String TYPE_COLUMN 			= "column_type";
    private final String WIDTH_COLUMN 			= "column_width";

    // Set column names
    private String[] columnNames = new String[] { CHECKED_COLUMN, NAME_COLUMN, TYPE_COLUMN, WIDTH_COLUMN };

    public ECLSprayFileDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (ECLSprayFile) jobEntryInt;
        if (this.jobEntry.getName() == null) {
            this.jobEntry.setName("ECLSprayJob");
        }
    }

    public JobEntryInterface open() {
        Shell parentShell = getParent();
        
        Display display = parentShell.getDisplay();

        shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        
       
       // ct = new CreateTable(shell);
       
        
        
      //  TabFolder tabFolder = new TabFolder (shell, SWT.FILL | SWT.RESIZE | SWT.MIN | SWT.MAX);
       // FormData data = new FormData();
        
       // data.height = 440;
        //data.width = 600;
        //tabFolder.setLayoutData(data);
        
      //  Composite compForGrp = new Composite(shell, SWT.NONE);
	//compForGrp.setLayout(new FillLayout(SWT.VERTICAL));
                
        
        //TabItem item1 = new TabItem(tabFolder, SWT.NULL);
        
       // item1.setText ("General");

       // Group grp = new Group(tabFolder, SWT.NONE );
       // grp.setLayoutData(new GridData(GridData.FILL_BOTH));
	//grp.setText("Test Group");
        
         
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


        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        shell.setLayout(formLayout);
        shell.setText("Define an ECL Dataset");

        FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;
        
        

        
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
        //Source Declaration
        Group sourceGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(sourceGroup);
        sourceGroup.setText("Landing Zone");
        sourceGroup.setLayout(groupLayout);
        FormData sourceGroupFormat = new FormData();
        sourceGroupFormat.top = new FormAttachment(generalGroup, margin);
        sourceGroupFormat.width = 400;
        sourceGroupFormat.height = 100;
        sourceGroupFormat.left = new FormAttachment(middle, 0);
        sourceGroup.setLayoutData(sourceGroupFormat);
       
        //ipAddress = buildText("IP Address", jobEntryName, lsMod, middle, margin, sourceGroup);
        filePath = buildText("File Name", jobEntryName, lsMod, middle, margin, sourceGroup);
        logicalFileName = buildText("Logical File Name", filePath, lsMod, middle, margin, sourceGroup);
        fileType = buildCombo("fileType", logicalFileName, lsMod, middle, margin, sourceGroup, new String[]{"Fixed", "Variable"});

        
        Group defGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook(defGroup);
        defGroup.setText("File Definition");
        defGroup.setLayout(groupLayout);
        FormData defGroupFormat = new FormData();
        defGroupFormat.top = new FormAttachment(sourceGroup, margin);
        defGroupFormat.width = 400;
        defGroupFormat.height = 150;
        defGroupFormat.left = new FormAttachment(middle, 0);
        defGroup.setLayoutData(defGroupFormat);
        
        Composite defComp = new Composite(defGroup, SWT.NONE);
        FormLayout defLayout = new FormLayout();
        defLayout.marginTop = 0;
        defLayout.marginHeight = 0;
	defComp.setLayout(defLayout);
        
        //CSV  Definition
        final Group csvGroup = new Group(defComp, SWT.SHADOW_NONE);
        props.setLook(csvGroup);
        csvGroup.setText("CSV");
        csvGroup.setLayout(groupLayout);
        final FormData csvGroupFormat = new FormData();
        csvGroupFormat.top = new FormAttachment(0, 0);
        csvGroupFormat.width = 380;
        csvGroupFormat.height = 100;
        csvGroupFormat.left = new FormAttachment(0, 0);
        csvGroup.setLayoutData(csvGroupFormat);

        csvSeperator = buildText("CSV Seperator", null, lsMod, middle, margin, csvGroup);
        csvTerminator = buildText("CSV Terminator", csvSeperator, lsMod, middle, margin, csvGroup);
        csvQuote = buildText("CSV Quote", csvTerminator, lsMod, middle, margin, csvGroup);

        //Fixed  Definition
        final Group fixedGroup = new Group(defComp, SWT.SHADOW_NONE);
        props.setLook(fixedGroup);
        fixedGroup.setText("Fixed Width");
        fixedGroup.setLayout(groupLayout);
        FormData fixedGroupFormat = new FormData();
        fixedGroupFormat.top = new FormAttachment(0, 0);
        fixedGroupFormat.width = 380;
        fixedGroupFormat.height = 100;
        fixedGroupFormat.left = new FormAttachment(0, 0);
        fixedGroup.setLayoutData(fixedGroupFormat);

        fixedRecordSize = buildText("Fixed Record Size", null, lsMod, middle, margin, fixedGroup);
        
       
        
        
        
        //item1.setControl(csvGroup);
        //item1.setControl(compForGrp);
        
        
       // TabItem item2 = ct.buildDefTab("Fields", tabFolder);
        
       /* if(jobEntry.getRecordList() != null){
            recordList = jobEntry.getRecordList();
            ct.setRecordList(jobEntry.getRecordList());
            
            if(recordList.getRecords() != null && recordList.getRecords().size() > 0) {
                    System.out.println("Size: "+recordList.getRecords().size());
                    for (Iterator<RecordBO> iterator = recordList.getRecords().iterator(); iterator.hasNext();) {
                            RecordBO obj = (RecordBO) iterator.next();
                            System.out.println("*******************");
                            System.out.println("Column Name: "+obj.getColumnName());
                            System.out.println("Column Type: "+obj.getColumnType());
                            if(obj.getColumnWidth() > 0)
                                    System.out.println("Height: "+obj.getColumnWidth());
                            System.out.println("*******************");

                    }
            }
        }
       
        */
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");

        BaseStepDialog.positionBottomButtons(shell, new Button[]{wOK, wCancel}, margin, defGroup);
        
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
        //if (jobEntry.getIpAddress() != null) {
         //   ipAddress.setText(jobEntry.getIpAddress());
        //}
        if (jobEntry.getFilePath() != null) {
            filePath.setText(jobEntry.getFilePath());
        }
        if (jobEntry.getLogicalFileName() != null) {
            logicalFileName.setText(jobEntry.getLogicalFileName());
        }
        if (jobEntry.getFileType() != null) {
            fileType.setText(jobEntry.getFileType());
        }
        if (jobEntry.getCsvSeperator() != null) {
            csvSeperator.setText(jobEntry.getCsvSeperator());
        }
        if (jobEntry.getCsvTerminator() != null) {
            csvTerminator.setText(jobEntry.getCsvTerminator());
        }
        if (jobEntry.getCsvQuote() != null) {
            csvQuote.setText(jobEntry.getCsvQuote());
        }
        if (jobEntry.getFixedRecordSize() != null) {
            fixedRecordSize.setText(jobEntry.getFixedRecordSize());
        }
        
        fileType.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				System.out.println("Item selected: "+fileType.getText());
				if(fileType.getText().equals("Variable")) {
					//ct.redrawTable(false);
                                        //fixedRecordSize.setVisible(false);
                                        fixedGroup.setVisible(false);
                                        csvGroup.setVisible(true);
                                        csvGroup.setLayoutData(csvGroupFormat);
                                        
                                        
				} else {
					//ct.redrawTable(true);
                                        fixedGroup.setVisible(true);
                                        csvGroup.setVisible(false);
                                        fixedGroup.setLayoutData(csvGroupFormat);
				}
				
			};
		});


        if(fileType.getText().equals("Variable")) {
                //ct.redrawTable(false);
                //fixedRecordSize.setVisible(false);
                fixedGroup.setVisible(false);
                csvGroup.setVisible(true);
                csvGroup.setLayoutData(csvGroupFormat);

        } else {
               // ct.redrawTable(true);
                fixedGroup.setVisible(true);
                csvGroup.setVisible(false);
                fixedGroup.setLayoutData(csvGroupFormat);
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

    private Text buildText(String strLabel, Control prevControl,
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

    private Text buildMultiText(String strLabel, Control prevControl,
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

    private Combo buildCombo(String strLabel, Control prevControl,
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

    private void ok() {
        jobEntry.setName(jobEntryName.getText());
       // jobEntry.setIpAddress(ipAddress.getText());
        jobEntry.setFilePath(filePath.getText());
        jobEntry.setLogicalFileName(logicalFileName.getText());
        jobEntry.setFileType(fileType.getText());
        jobEntry.setCsvSeperator(csvSeperator.getText());
        jobEntry.setCsvQuote(csvQuote.getText());
        jobEntry.setCsvTerminator(csvTerminator.getText());
        jobEntry.setFixedRecordSize(fixedRecordSize.getText());
        //jobEntry.setRecordList(ct.getRecordList());
        dispose();
    }

    private void cancel() {
        jobEntry.setChanged(backupChanged);
        jobEntry = null;
        dispose();
    }

    public void dispose() {
        WindowProperty winprop = new WindowProperty(shell);
        props.setScreen(winprop);
        shell.dispose();
    }
}

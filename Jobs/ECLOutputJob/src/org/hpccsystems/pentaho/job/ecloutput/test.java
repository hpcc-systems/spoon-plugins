/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecloutput;
//Send questions, comments, bug reports, etc. to the authors:

//Rob Warner (rwarner@interspatial.com)
//Robert Harris (rbrt_harris@yahoo.com)
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.*;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.*;

import org.eclipse.swt.widgets.*;
import org.hpccsystems.ecldirect.Dataset;

/**
 * This class demonstrates ScrolledComposite
 */
public class test {

    private Text jobEntryName;
    private Button wOK, wCancel, fileOpenButton;
    private boolean backupChanged;
    private SelectionAdapter lsDef;
    private Combo attributeName;
    private Combo isDef; //true set output into attr using job entry name
    private Combo outputType; //recordset,expression
    private Combo includeFormat; //yes/no
    private Combo type; //thor file options, CSV, XML, pipe, Named, File Owned by workunit
    private Text format;
    //private Text recordset;
    private Text expression;
    private Text file;
    private Text options; // used for CSV, XML, Pipe
    private Text fileOptions; // used for CSV, XML
    private Text name; //used for named
    private Text extend; // availiable for Named
    private Combo returnAll; // availiable for Named
    private Combo thor; // used in 

    
    private Group expressionGroup;
    private Group outputGroup;
    private Group generalGroup;
    private Group recordsetGroup;
    private Group recordsetDetailsGroup;
    private Group fileTypeGroup;
    private Composite c1;
    
    
    
    public void run() {
        Display display = new Display();
        Shell shell = new Shell(display);
        shell.setSize(600, 400);
        createContents(shell);
        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        display.dispose();
    }

    private void createContents(Composite parent) {
    }

    private void createContents2(Composite parent) {
        GridData gridData = new GridData(GridData.HORIZONTAL_ALIGN_FILL | GridData.FILL_BOTH);
        GridLayout layout = new GridLayout(1, false);
        layout.marginWidth = 1;
        parent.setLayout(layout);
        parent.setLayoutData(gridData);


        Group generalGroup = new Group(parent, SWT.SHADOW_NONE);
        generalGroup.setSize(200, 200);
        generalGroup.setText("General Details");


        //Group

        // Create the ScrolledComposite to scroll horizontally and vertically
        ScrolledComposite sc = new ScrolledComposite(generalGroup, SWT.H_SCROLL
                | SWT.V_SCROLL);

        // Create a child composite to hold the controls
        Composite child = new Composite(sc, SWT.NONE);
        child.setLayout(new GridLayout());


        // Create the buttons
        new Button(child, SWT.PUSH).setText("One");
        new Button(child, SWT.PUSH).setText("Two");

        // Set the child as the scrolled content of the ScrolledComposite
        sc.setContent(child);

        // Set the minimum size
        sc.setMinSize(400, 400);

        // Expand both horizontally and vertically
        sc.setExpandHorizontal(true);
        sc.setExpandVertical(true);

    }

    public static void main2(String[] args) {
        new test().run();
    }

    private void createGeneralGroup(Composite parent, FormLayout groupLayout, ModifyListener lsMod) {
        int middle = 50;
        final int margin = 10;
        generalGroup = new Group(parent, SWT.SHADOW_NONE);
        generalGroup.setBackground(new Color(generalGroup.getDisplay(),255,255,255));
        generalGroup.setText("General Details");
        generalGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        groupFormat.minimumHeight = 170;
        groupFormat.heightHint = 170;
        groupFormat.widthHint = 530;
        generalGroup.setLayoutData(groupFormat);
        generalGroup.setSize(generalGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        generalGroup.layout();

        jobEntryName = buildText("Job Entry Name", null, lsMod, middle, margin, generalGroup);
        this.isDef = buildCombo("Is definition", jobEntryName, lsMod, middle, margin, generalGroup, new String[]{"", "Yes", "No"});
        this.outputType = buildCombo("Input Type", isDef, lsMod, middle, margin, generalGroup, new String[]{"", "Recordset", "Expression"});
        this.includeFormat = buildCombo("Include Format", outputType, lsMod, middle, margin, generalGroup, new String[]{"", "Yes", "No"});
        
        

    }

    private void createOutputGroup(String[] datasets, Composite parent, FormLayout groupLayout, ModifyListener lsMod) {
        int middle = 50;
        final int margin = 10;
        outputGroup = new Group(parent, SWT.SHADOW_NONE);
        outputGroup.setBackground(new Color(outputGroup.getDisplay(),255,255,255));
        outputGroup.setText("Output Details");
        outputGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        groupFormat.minimumHeight = 65;
        groupFormat.heightHint = 55;
        groupFormat.widthHint = 530;
        outputGroup.setLayoutData(groupFormat);
        outputGroup.setSize(outputGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        outputGroup.layout();



        this.attributeName = buildCombo("Recordset Name", null, lsMod, middle, margin, outputGroup, datasets);

    }

    private void createExpressionGroup(Composite parent, FormLayout groupLayout, ModifyListener lsMod) {
        int middle = 45;
        final int margin = 20;
        this.expressionGroup = new Group(parent, SWT.SHADOW_NONE);
        expressionGroup.setBackground(new Color(expressionGroup.getDisplay(),255,255,255));
        expressionGroup.setText("Expression Details");
        expressionGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        groupFormat.minimumHeight = 265;
        groupFormat.heightHint = 265;
        groupFormat.widthHint = 530;
        expressionGroup.setLayoutData(groupFormat);
        expressionGroup.setSize(expressionGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        expressionGroup.layout();



        expression = buildMultiText("Expression", null, lsMod, middle, margin, this.expressionGroup);
        name = buildText("Name", expression, lsMod, middle, margin, this.expressionGroup);

    }
    
    
    private void createRecordsetGroup(Composite parent, final FormLayout groupLayout, final ModifyListener lsMod) {
        int middle = 45;
        final int margin = 20;
        recordsetGroup = new Group(parent, SWT.SHADOW_NONE);
        recordsetGroup.setBackground(new Color(recordsetGroup.getDisplay(),255,255,255));
        recordsetGroup.setText("Recordset Details");
        recordsetGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        groupFormat.minimumHeight = 65;
        groupFormat.heightHint = 65;
        groupFormat.widthHint = 530;
        recordsetGroup.setLayoutData(groupFormat);
        recordsetGroup.setSize(recordsetGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        recordsetGroup.layout();
        
        this.thor = buildCombo("File on disk, \"owned\" by workunit", null, lsMod, middle, margin, recordsetGroup,new String[]{"", "Yes", "No"});

        this.thor.addSelectionListener(new SelectionAdapter(){
                public void widgetSelected(SelectionEvent e) {
                            
                            if(fileTypeGroup != null){
                               fileTypeGroup.dispose();
                            }
                            if(recordsetDetailsGroup != null){
                               recordsetDetailsGroup.dispose();
                            }
                    if(thor.getText().equals("Yes")) {

                    } else if(thor.getText().equals("No")){
                       createRecordsetDetailsGroup(c1, groupLayout, lsMod);
                    }
                    c1.setSize(c1.computeSize(SWT.DEFAULT, SWT.DEFAULT));
                    c1.layout();
                };
                
            
            });

       
    }
    
    private void createRecordsetDetailsGroup(Composite parent, final FormLayout groupLayout, final ModifyListener lsMod) {
        int middle = 45;
        final int margin = 20;
        recordsetDetailsGroup = new Group(parent, SWT.SHADOW_NONE);
        recordsetDetailsGroup.setBackground(new Color(recordsetDetailsGroup.getDisplay(),255,255,255));
        recordsetDetailsGroup.setText("Recordset Output Details");
        recordsetDetailsGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        groupFormat.minimumHeight = 105;
        groupFormat.heightHint = 105;
        groupFormat.widthHint = 530;
        recordsetDetailsGroup.setLayoutData(groupFormat);
        recordsetDetailsGroup.setSize(recordsetDetailsGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        recordsetDetailsGroup.layout();


        this.format = buildText("Format", null, lsMod, middle, margin, recordsetDetailsGroup);
        this.type = buildCombo("Type", this.format, lsMod, middle, margin, recordsetDetailsGroup,new String[]{"", "File", "File - CSV", "File - XML", "Piped", "Named"});
        
        
        this.type.addSelectionListener(new SelectionAdapter() {
                    public void widgetSelected(SelectionEvent e) {
                            if(fileTypeGroup != null){
                               fileTypeGroup.dispose();
                            }
                            if(!type.getText().equals("")){
                                createFileTypeGroup(type.getText(), c1, groupLayout, lsMod);
                            }
                        c1.setSize(c1.computeSize(SWT.DEFAULT, SWT.DEFAULT));
                        c1.layout();
                    };
            });
       
    }
    
    private void createFileTypeGroup(String type, Composite parent, FormLayout groupLayout, ModifyListener lsMod) {
        int middle = 45;
        final int margin = 20;
        fileTypeGroup = new Group(parent, SWT.SHADOW_NONE);
        fileTypeGroup.setBackground(new Color(fileTypeGroup.getDisplay(),255,255,255));
        fileTypeGroup.setText(type + " Details");
        fileTypeGroup.setLayout(groupLayout);
        GridData groupFormat = new GridData(SWT.FILL);
        groupFormat.minimumWidth = 530;
        
        groupFormat.widthHint = 530;
        
        int height = 135;
       // {"", "File", "File - CSV", "File - XML", "Piped", "Named"}
        if(type.equals("File")){
            height = 105;
            this.file = buildText("File", null, lsMod, middle, margin, fileTypeGroup);
            this.fileOptions = buildText("Thor File Options", this.file, lsMod, middle, margin, fileTypeGroup);
            
        }else if(type.equals("File - CSV")){
            this.file = buildText("File", null, lsMod, middle, margin, fileTypeGroup);
            this.fileOptions = buildText("CSV File Options", this.file, lsMod, middle, margin, fileTypeGroup);
            this.options = buildText("CSV Options", this.fileOptions, lsMod, middle, margin, fileTypeGroup);
        }else if(type.equals("File - XML")){
            this.file = buildText("File", null, lsMod, middle, margin, fileTypeGroup);
            this.fileOptions = buildText("XML File Options", this.file, lsMod, middle, margin, fileTypeGroup);
            this.options = buildText("XML Options", this.fileOptions, lsMod, middle, margin, fileTypeGroup);
        }else if (type.equals("Piped")){
            height = 65;
            this.options = buildText("Pipe Options", this.fileOptions, lsMod, middle, margin, fileTypeGroup);
        }else if (type.equals("Named")){
            height = 65;
            this.name = buildText("Name", null, lsMod, middle, margin, fileTypeGroup);
           
        }

        groupFormat.minimumHeight = height;
        groupFormat.heightHint = height;
        fileTypeGroup.setLayoutData(groupFormat);
        fileTypeGroup.setSize(fileTypeGroup.computeSize(SWT.DEFAULT, SWT.DEFAULT));
        fileTypeGroup.layout();
        
       
    }

    public static void main(String[] args) {
        final test t = new test();
        
        t.runGUI();
    }
    
    private void runGUI(){
        final String datasets[] = new String[]{"", "a", "b"};
        Display display = new Display();
        Color red = display.getSystemColor(SWT.COLOR_RED);
        Color blue = display.getSystemColor(SWT.COLOR_BLUE);
        Shell shell = new Shell(display);
        shell.setBackground(new Color(shell.getDisplay(),255,255,255));
        GridLayout fl = new GridLayout(2,false);
        
        shell.setLayout(fl);
        GridData gd = new GridData();
        gd.horizontalSpan = 2;


        final ModifyListener lsMod = new ModifyListener() {

            public void modifyText(ModifyEvent e) {
                //jobEntry.setChanged();
            }
        };
        final FormLayout groupLayout = new FormLayout();
        groupLayout.marginWidth = 10;
        groupLayout.marginHeight = 10;
        

        final ScrolledComposite sc1 = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.BORDER | SWT.SHADOW_OUT);
        sc1.setBackground(new Color(sc1.getDisplay(),255,255,255));
        sc1.setLayoutData(gd);
        sc1.setAlwaysShowScrollBars(true);
        sc1.setSize(200,200);
        c1 = new Composite(sc1, SWT.NONE);
        sc1.setContent(c1);
        //c1.setBackground(red);
        GridLayout layout = new GridLayout();
        layout.marginRight = 15;
        layout.numColumns = 1;
        
        c1.setLayout(layout);
        c1.setBackground(new Color(c1.getDisplay(),255,255,255));

        c1.setSize(595, 400);
        
        this.createGeneralGroup(c1, groupLayout, lsMod);
        this.createOutputGroup(datasets, c1, groupLayout, lsMod);
        //Point dPoint = c1.computeSize(SWT.DEFAULT, SWT.DEFAULT);
        
        //c1.setSize(dPoint);
        c1.setSize(new Point(560,400));

/*
        Button ok = new Button(shell, SWT.PUSH);
        ok.setText("OK");
         Button cancel = new Button(shell, SWT.PUSH);
        cancel.setText("Cancel");
  */      
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText("OK");
        
        GridData okGrid = new GridData();
         okGrid.widthHint = 55;
         okGrid.heightHint = 24;
        //okGrid.minimumWidth = 200;
        okGrid.horizontalIndent = 250;
        wOK.setLayoutData(okGrid);
        
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText("Cancel");
        GridData cancelGrid = new GridData();
        cancelGrid.widthHint = 55;
        cancelGrid.heightHint = 24;
        wCancel.setLayoutData(cancelGrid);
        
        this.outputType.addSelectionListener(new SelectionAdapter() {
                    public void widgetSelected(SelectionEvent e) {
                            if(expressionGroup != null){
                               expressionGroup.dispose();
                            }

                            if(recordsetGroup != null){
                                recordsetGroup.dispose();
                            }
                            if(fileTypeGroup != null){
                               fileTypeGroup.dispose();
                            }
                            if(recordsetDetailsGroup != null){
                               recordsetDetailsGroup.dispose();
                            }
                            

                            if(outputType.getText().equals("Recordset")) {
                                createRecordsetGroup(c1, groupLayout, lsMod);
                            } else if(outputType.getText().equals("Expression")){
                                
                                createExpressionGroup(c1, groupLayout, lsMod);
                            }
                        c1.setSize(c1.computeSize(SWT.DEFAULT, SWT.DEFAULT));
                        c1.layout();
                    };
            });
        
        
        
         
        shell.setSize(600, 500);
        
        shell.open();

        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        display.dispose();
    }

    private Button buildButton(String strLabel, Control prevControl,
            ModifyListener isMod, int middle, int margin, Composite groupBox) {

        Button nButton = new Button(groupBox, SWT.PUSH | SWT.SINGLE | SWT.CENTER);
        nButton.setText(strLabel + "Test");
        // props.setLook(nButton);
        //nButton.addModifyListener(lsMod)
        FormData fieldFormat = new FormData();
        fieldFormat.left = new FormAttachment(middle, 0);
        fieldFormat.top = new FormAttachment(prevControl, margin);
        fieldFormat.right = new FormAttachment(75, 0);
        fieldFormat.height = 25;
        
        nButton.setLayoutData(fieldFormat);

        return nButton;


    }

    private String buildFileDialog(Shell shell) {

        //file field
        FileDialog fd = new FileDialog(shell, SWT.SAVE);

        fd.setText("Save");
        fd.setFilterPath("C:/");
        String[] filterExt = {"*.csv", ".xml", "*.txt", "*.*"};
        fd.setFilterExtensions(filterExt);
        String selected = fd.open();
        if (fd.getFileName() != "") {
            return fd.getFilterPath() + System.getProperty("file.separator") + fd.getFileName();
        } else {
            return "";
        }

    }

    private Text buildText(String strLabel, Control prevControl,
            ModifyListener lsMod, int middle, int margin, Composite groupBox) {
        // label
        Label fmt = new Label(groupBox, SWT.RIGHT);
        fmt.setText(strLabel);
        //props.setLook(fmt);
        FormData labelFormat = new FormData();
        labelFormat.left = new FormAttachment(0, 0);
        labelFormat.top = new FormAttachment(prevControl, margin);
        labelFormat.right = new FormAttachment(middle, -margin);
        fmt.setLayoutData(labelFormat);

        // text field
        Text text = new Text(groupBox, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        //props.setLook(text);
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
        //props.setLook(fmt);
        FormData labelFormat = new FormData();
        labelFormat.left = new FormAttachment(0, 0);
        labelFormat.top = new FormAttachment(prevControl, margin);
        labelFormat.right = new FormAttachment(middle, -margin);
        fmt.setLayoutData(labelFormat);

        // text field
        Text text = new Text(groupBox, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL);
        //props.setLook(text);
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
        //props.setLook(fmt);
        FormData labelFormat = new FormData();
        labelFormat.left = new FormAttachment(0, 0);
        labelFormat.top = new FormAttachment(prevControl, margin);
        labelFormat.right = new FormAttachment(middle, -margin);
        fmt.setLayoutData(labelFormat);

        // combo field
        Combo combo = new Combo(groupBox, SWT.MULTI | SWT.LEFT | SWT.BORDER);
        //props.setLook(combo);
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
}
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hpccsystems.pentaho.job.ecloutliers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.ComboBoxCellEditor;
import org.eclipse.jface.viewers.ICellModifier;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TextCellEditor;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
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
import org.eclipse.swt.widgets.Item;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;
import org.pentaho.di.core.Const;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import org.hpccsystems.eclguifeatures.AutoPopulate;
import org.hpccsystems.eclguifeatures.ErrorNotices;
import org.hpccsystems.mapper.MainMapper;
import org.hpccsystems.mapper.MainMapperForOutliers;
import org.hpccsystems.mapper.MapperRecordList;
import org.hpccsystems.mapper.Utils;
import org.hpccsystems.recordlayout.RecordLabels;
import org.hpccsystems.recordlayout.RecordList;
import org.hpccsystems.ecljobentrybase.*;

/**
 * 
 * @author KeshavS
 */
public class ECLOutliersDialog extends ECLJobEntryDialog {

	public static final String NAME = "Name";
	public static final String MIN = "Min";
	public static final String MAX = "Max";

	public static final String[] PROP = { NAME, MIN, MAX };
	// Map<String, String[]> mapDataSets = null;
	java.util.List people;
	private ECLOutliers jobEntry;
	private Text jobEntryName;
	private Combo datasetName;
	ArrayList<String> Fieldfilter = new ArrayList<String>();

	private MainMapperForOutliers tblMapper = null;
	private RecordList recordList = new RecordList();
	private MapperRecordList mapperRecList = new MapperRecordList();
	private Button wOK, wCancel, wSave;
	private boolean backupChanged;
	private String[] dataType;
	/*
	 * public Button chkBox; public static Text outputName; public static Label
	 * label; private String persist;
	 */
	private Composite composite;
	private SelectionAdapter lsDef;
	private Button testButton;

	private List rules = new ArrayList();

	public List<String> getRulesList() {
		return rules;
	}

	public void setRulesList() {
		this.rules = jobEntry.getRulesList();
	}

	public String[] getDataType() {
		return dataType;
	}

	public void setDataType(String[] dataType) {
		this.dataType = dataType;
	}

	public ECLOutliersDialog(Shell parent, JobEntryInterface jobEntryInt,
			Repository rep, JobMeta jobMeta) {
		super(parent, jobEntryInt, rep, jobMeta);
		jobEntry = (ECLOutliers) jobEntryInt;
		if (this.jobEntry.getName() == null) {
			this.jobEntry.setName("Outliers");
		}
	}

	// Building GUI
	public JobEntryInterface open() {
		Shell parentShell = getParent();
		final Display display = parentShell.getDisplay();

		String datasets[] = null;

		final AutoPopulate ap = new AutoPopulate();
		try {
			// Object[] jec = this.jobMeta.getJobCopies().toArray();

			datasets = ap.parseDatasetsRecordsets(this.jobMeta.getJobCopies());

		} catch (Exception e) {
			System.out.println("Error Parsing existing Datasets");
			System.out.println(e.toString());
			datasets = new String[] { "" };
		}

		shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN
				| SWT.MAX);
		people = new ArrayList();

		TabFolder tab = new TabFolder(shell, SWT.FILL | SWT.RESIZE | SWT.MIN
				| SWT.MAX);
		FormData datatab = new FormData();

		datatab.height = 530;
		datatab.width = 750;
		tab.setLayoutData(datatab);

		Composite compForGrp = new Composite(tab, SWT.NONE);
		// compForGrp.setLayout(new FillLayout(SWT.VERTICAL));
		compForGrp.setBackground(new Color(tab.getDisplay(), 255, 255, 255));
		compForGrp.setLayout(new FormLayout());
		TabItem item1 = new TabItem(tab, SWT.NULL);

		item1.setText("General");
		props.setLook(shell);
		JobDialog.setShellImage(shell, jobEntry);

		ModifyListener lsMod = new ModifyListener() {

			public void modifyText(ModifyEvent e) {
				jobEntry.setChanged();
			}
		};
		backupChanged = jobEntry.hasChanged();

		FormLayout layout = new FormLayout();
		layout.marginWidth = Const.FORM_MARGIN;
		layout.marginHeight = Const.FORM_MARGIN;

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		shell.setLayout(layout);
		shell.setText("Outliers");

		FormLayout groupLayout = new FormLayout();
		groupLayout.marginWidth = 10;
		groupLayout.marginHeight = 10;

		// Stepname line
		Group generalGroup = new Group(compForGrp, SWT.SHADOW_NONE | SWT.CENTER);
		props.setLook(generalGroup);
		generalGroup.setText("General Details");
		generalGroup.setLayout(groupLayout);
		FormData generalGroupFormat = new FormData();
		generalGroupFormat.top = new FormAttachment(10, margin);
		generalGroupFormat.width = 400;
		generalGroupFormat.height = 130;
		generalGroupFormat.left = new FormAttachment(30, 0);
		generalGroup.setLayoutData(generalGroupFormat);

		jobEntryName = buildText("Job Entry Name :    ", null, lsMod, middle,
				margin, generalGroup);

		// All other contols
		// Dataset Declaration
		Group datasetGroup = new Group(compForGrp, SWT.SHADOW_NONE);
		props.setLook(datasetGroup);
		datasetGroup.setText("Dataset & Frequency Details");
		datasetGroup.setLayout(groupLayout);
		FormData datasetGroupFormat = new FormData();
		datasetGroupFormat.top = new FormAttachment(generalGroup, margin);
		datasetGroupFormat.width = 400;
		datasetGroupFormat.height = 130;
		datasetGroupFormat.left = new FormAttachment(30, 0);
		datasetGroup.setLayoutData(datasetGroupFormat);

		datasetName = buildCombo("Dataset Name:    ", jobEntryName, lsMod,
				middle, margin, datasetGroup, datasets);

		// listner

		item1.setControl(compForGrp);

		// Start
		TabItem item2 = new TabItem(tab, SWT.NULL);
		ScrolledComposite sc2 = new ScrolledComposite(tab, SWT.H_SCROLL
				| SWT.V_SCROLL);
		final Composite compForGrp2 = new Composite(sc2, SWT.NONE);
		// compForGrp2.setBackground(new Color(tab.getDisplay(),255,255,255));
		// compForGrp2.setLayout(new FormLayout());
		sc2.setContent(compForGrp2);

		// Set the minimum size
		sc2.setMinSize(650, 450);

		// Expand both horizontally and vertically
		sc2.setExpandHorizontal(true);
		sc2.setExpandVertical(true);

		item2.setText("Rule");
		item2.setControl(sc2);

		GridLayout mapperCompLayout = new GridLayout();
		mapperCompLayout.numColumns = 1;
		GridData mapperCompData = new GridData();
		mapperCompData.grabExcessHorizontalSpace = true;
		compForGrp2.setLayout(mapperCompLayout);
		compForGrp2.setLayoutData(mapperCompData);

		Map<String, String[]> mapDataSets = null;
		try {
			mapDataSets = ap.parseDefEDAExpressionBuilder(this.jobMeta.getJobCopies());
		} catch (Exception e) {
			e.printStackTrace();
		}

		final List<JobEntryCopy> jobs = this.jobMeta.getJobCopies();
		// Create a Mapper
		String[] cmbValues = { "" };
		tblMapper = new MainMapperForOutliers(display, compForGrp2,
				mapDataSets, cmbValues, "outliers", jobs, getDataType());
		if (jobEntry.getFilterStatement() != null) {
			tblMapper.setFilterStatement(jobEntry.getFilterStatement());// populate
																		// current
			tblMapper.setOldFilterStatement(jobEntry.getFilterStatement());// populate
																			// old
																			// if
																			// save
																			// isn't
																			// hit
			System.out.println("opening filter statement : "
					+ tblMapper.getFilterStatement());
		}

		// tblMapper.setLayoutStyle("filter");
		// Add the existing Mapper RecordList
		if (jobEntry.getMapperRecList() != null) {
			mapperRecList = jobEntry.getMapperRecList();
			tblMapper.setMapperRecList(jobEntry.getMapperRecList());
		}

		if (jobEntry.getRulesList() != null) {
			tblMapper.setRulesList(jobEntry.getRulesList());// populate current
			System.out.println("opening Rules : " + tblMapper.getRulesList());
		}

		tblMapper.reDrawTable();

		datasetName.addModifyListener(new ModifyListener() {

			@Override
			public void modifyText(ModifyEvent arg0) {
				// AutoPopulate ap = new AutoPopulate();
				try {
					String[] items = ap.fieldsRecByDataset( datasetName.getText(),jobMeta.getJobCopies());
		              //rec = ap.rawFieldsByDataset( datasetName.getText(),jobMeta.getJobCopies());
					RecordList fields = ap.buildMyRecordList(items);
					//RecordList fields = ap.rawFieldsByDataset(datasetName.getText(), jobMeta.getJobCopies());
					dataType = new String[fields.getRecords().size()];
					for (int i = 0; i < fields.getRecords().size(); i++) {
						try {
							dataType[i] = fields.getRecords().get(i)
									.getColumnType();
						} catch (Exception e) {
							System.out
									.println("Frequency Cant look up column type");
						}
					}
					// tblOutput.setParentLayout(fields);
				} catch (Exception e) {
					System.out.println(e);
				}
				setDataType(dataType);

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
				ok();
			}
		};

		Listener saveListener = new Listener() {

			@Override
			public void handleEvent(Event arg0) {
				FileDialog fd = new FileDialog(shell, SWT.SAVE);
				fd.setText("Save");
				fd.setFilterPath(".");
				String[] filterExt = { "*.txt", "*.doc", ".rtf", "*.*" };
				fd.setFilterExtensions(filterExt);
				String selected = fd.open();
				File file = new File(selected);
				if (file.exists()) {
					file.delete();
				}
				try {
					file.createNewFile();
					FileWriter fw = new FileWriter(file.getAbsoluteFile());
					BufferedWriter bw = new BufferedWriter(fw);
					for (Iterator<String> it = tblMapper.getRulesList().iterator(); it.hasNext();) {
						String rule = (String) it.next();
						bw.write(rule.trim() + "\n");
					}
					bw.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}

			}
		};
		// End

		wSave = new Button(shell, SWT.PUSH);
		wSave.setText("Save Rules");
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText("OK");
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText("Cancel");

		BaseStepDialog.positionBottomButtons(shell, new Button[] { wSave, wOK,
				wCancel }, margin, tab);

		wCancel.addListener(SWT.Selection, cancelListener);
		wOK.addListener(SWT.Selection, okListener);
		wSave.addListener(SWT.Selection, saveListener);

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

		this.datasetName.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				System.out.print("inRecordName Listner");
				try {
					populateDatasets(jobs);
				} catch (Exception excep) {
					System.out.println("Failed to load datasets");
				}
			};
		});

		if (jobEntry.getName() != null) {
			jobEntryName.setText(jobEntry.getName());
		}

		if (jobEntry.getDatasetName() != null) {
			datasetName.setText(jobEntry.getDatasetName());
			try {
				populateDatasets(jobs);
			} catch (Exception e) {
				System.out.println("Failed to load datasets");
			}

		}

		if (jobEntry.getDatasetName() != null) {
			datasetName.setText(jobEntry.getDatasetName());
		}

		if (jobEntry.getDataType() != null) {
			dataType = new String[jobEntry.getDataType().length];
			dataType = jobEntry.getDataType();
			setDataType(dataType);
		}

		/*
		 * if (jobEntry.getPersistOutputChecked() != null && chkBox != null) {
		 * chkBox
		 * .setSelection(jobEntry.getPersistOutputChecked().equals("true")?
		 * true:false); }
		 * 
		 * if(chkBox != null && chkBox.getSelection()){ for (Control control :
		 * composite_1.getChildren()) { if(!control.isDisposed()){ if
		 * (jobEntry.getOutputName() != null && outputName != null) {
		 * outputName.setText(jobEntry.getOutputName()); } if
		 * (jobEntry.getLabel() != null && label != null) {
		 * label.setText(jobEntry.getLabel()); } } } }
		 */

		shell.pack();
		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		return jobEntry;
	}

	private void populateDatasets(List<JobEntryCopy> jobs) throws Exception {
		AutoPopulate ap = new AutoPopulate();
		Map<String, String[]> mapDataSets = null;
		try {
			mapDataSets = ap.parseDefEDAExpressionBuilder(this.jobMeta.getJobCopies(), datasetName.getText());
		} catch (Exception e) {
			e.printStackTrace();
		}
		// (tblMapper.getTreeInputDataSet()).clearAll(false);
		(tblMapper.getTreeInputDataSet()).removeAll();
		Utils.fillMyTree(tblMapper.getTreeInputDataSet(), mapDataSets, false,
				getDataType(), jobs);

		tblMapper.reDrawTable();

	}

	private boolean validate() {
		boolean isValid = true;
		String errors = "";

		if (this.jobEntryName.getText().equals("")) {
			isValid = false;
			errors += "\"Job Entry Name\" is a required field!\r\n";
		}
		if (this.datasetName.getText().equals("")) {
			isValid = false;
			errors += "\"Dataset Name\" is a required field!\r\n";
		}

		if (!isValid) {
			ErrorNotices en = new ErrorNotices();
			errors += "\r\n";
			errors += "If you continue to save with errors you may encounter compile errors if you try to execute the job.\r\n\r\n";
			isValid = en.openValidateDialog(getParent(), errors);
		}
		return isValid;

	}

	private void ok() {

		if (!validate()) {
			return;
		}
		jobEntry.setName(jobEntryName.getText());
		jobEntry.setDatasetName(this.datasetName.getText());
		jobEntry.setRulesList(tblMapper.getRulesList());
		jobEntry.setDataType(this.dataType);
		jobEntry.setFilterStatement(tblMapper.getFilterStatement());
		System.out.println("setting filter statement : "
				+ tblMapper.getFilterStatement());
		tblMapper.setOldexpression(tblMapper.getFilterStatement());// update
																	// filter
																	// statement
																	// that its
																	// changed
																	// and oked
		/*
		 * if(chkBox.getSelection() && outputName != null){
		 * jobEntry.setOutputName(outputName.getText()); }
		 * 
		 * if(chkBox.getSelection() && label != null){
		 * jobEntry.setLabel(label.getText()); }
		 * 
		 * if(chkBox != null){
		 * jobEntry.setPersistOutputChecked(chkBox.getSelection
		 * ()?"true":"false"); }
		 */
		dispose();
	}

	private void cancel() {
		jobEntry.setChanged(backupChanged);
		jobEntry = null;
		dispose();
	}

}

class Cols {
	private String firstName;

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

}

class PersonCellModifier implements ICellModifier {
	private Viewer viewer;

	public PersonCellModifier(Viewer viewer) {
		this.viewer = viewer;
	}

	public boolean canModify(Object element, String property) {
		// Allow editing of all values
		return true;
	}

	public Object getValue(Object element, String property) {
		Player p = (Player) element;
		if (ECLOutliersDialog.NAME.equals(property))
			return p.getFirstName();
		else if (ECLOutliersDialog.MIN.equals(property))
			return p.getMin();
		else if (ECLOutliersDialog.MAX.equals(property))
			return p.getMax();
		else
			return null;
	}

	public void modify(Object element, String property, Object value) {
		if (element instanceof Item)
			element = ((Item) element).getData();

		Player p = (Player) element;
		if (ECLOutliersDialog.NAME.equals(property))
			p.setFirstName((String) value);
		else if (ECLOutliersDialog.MIN.equals(property))
			p.setMin((String) value);
		else if (ECLOutliersDialog.MAX.equals(property))
			p.setMax((String) value);
		// Force the viewer to refresh
		viewer.refresh();
	}
}

class Player {
	private String firstName;
	private String min;
	private String max;

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getMin() {
		return min;
	}

	public void setMin(String min) {
		this.min = min;
	}

	public String getMax() {
		return max;
	}

	public void setMax(String max) {
		this.max = max;
	}

}

class PlayerLabelProvider implements ITableLabelProvider {

	// Constructs a PlayerLabelProvider
	public PlayerLabelProvider() {
	}

	public Image getColumnImage(Object arg0, int arg1) {

		return null;
	}

	public String getColumnText(Object arg0, int arg1) {
		Player values = (Player) arg0;
		// String text = "";
		switch (arg1) {
		case 0:
			return values.getFirstName();// text = values[0];
			// break;
		case 1:
			return values.getMin();// text = values[1];
		case 2:
			return values.getMax();// text = values[1];
			// break;
		}
		return null;
	}

	public void addListener(ILabelProviderListener arg0) {
		// Throw it away
	}

	public void dispose() {

	}

	public boolean isLabelProperty(Object arg0, String arg1) {
		return false;
	}

	public void removeListener(ILabelProviderListener arg0) {
		// Do nothing
	}
}

class PlayerContentProvider implements IStructuredContentProvider {

	public Object[] getElements(Object arg0) {

		return ((List) arg0).toArray();
	}

	public void dispose() {
		// We don't create any resources, so we don't dispose any
	}

	public void inputChanged(Viewer arg0, Object arg1, Object arg2) {
		// Nothing to do
	}
}

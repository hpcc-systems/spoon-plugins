package com.lexisnexis.ui.clustersources;

import org.eclipse.jface.viewers.ColumnLabelProvider;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TableViewerColumn;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.jface.viewers.ViewerCell;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.TableEditor;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;

import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import com.lexisnexis.ui.constants.Constants;

public class ClusterSourcesTable {
	
	private TableViewer viewer;
	private ClusterSourcesRecordList datalist;
	
	public ClusterSourcesTable(String fileName){
		datalist = new ClusterSourcesRecordList(fileName);
	}
	
	public void run() {
		Display display = new Display();
	    Shell shell = new Shell(display);
	    shell.setLayout(new FillLayout());
	    configureShell(shell);
	    final CTabFolder folder = new CTabFolder(shell, SWT.BORDER);
	    createContents(folder);
	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	    display.dispose();
	}
	

	protected void configureShell(Shell shell) {
		shell.setText(Constants.CLUSTER_SOURCES_REPORT_TITLE);
		shell.setSize(950, 400);
	}
	
	/**
	 * Add the columns to the table and add the listener
	 * @param table
	 * @param parent
	 */
	public void addColumns(Table table, Shell parent){
		for(int i = 0; i < Constants.ARRAY_CLUSTER_SOURCES_COL_HEADERS.length; i++){
			TableColumn item = new TableColumn(table, SWT.LEFT);
			item.setText(Constants.ARRAY_CLUSTER_SOURCES_COL_HEADERS[i]);
			if(Constants.ARRAY_CLUSTER_SOURCES_COL_HEADERS[i].equals(Constants.CLUSTER_SOURCES)) {
				item.setWidth(300);
				new LinkListener(viewer, item).displayLink(parent.getShell(), table, Constants.CLUSTER_SOURCES, Constants.ISCOL);
			} else {
				item.setWidth(150);
			} 
		}
	}
	
	/**
	 * Create a Table and add Label and Content providers
	 * @param parent
	 * @return Control
	 */
	//protected Control createContents(final Composite parent) {
	//	Composite composite = new Composite(parent, SWT.NONE);
	//	composite.setLayout(new FillLayout());
	public void createContents(CTabFolder folder) {
		
		CTabItem item = new CTabItem(folder, SWT.CLOSE);
		item.setText(Constants.CLUSTER_SOURCES_REPORT_TITLE);
		
		Composite comp = new Composite(folder, SWT.NONE);
		comp.setLayout(new FillLayout());
		
		viewer = new TableViewer(comp);
		viewer.setContentProvider(new ClusterSourcesContentProvider());
		viewer.setLabelProvider(new ClusterSourcesLabelProvider());
		
		final Table table = viewer.getTable();
		
		addColumns(table, comp.getShell());	//Add Column Headers and Link Listener to table
	    viewer.setInput(datalist);
	    
	    table.setHeaderVisible(true);
	    table.setLinesVisible(true);
	    item.setControl(comp);
		//return composite;
	}
	
	
	public class ClusterSourcesContentProvider implements IStructuredContentProvider{

		@Override
		public void dispose() {
			// Nothing to dispose
			
		}

		@Override
		public void inputChanged(Viewer arg0, Object arg1, Object arg2) {
			// Throw it away
			
		}

		@Override
		public Object[] getElements(Object arg0) {
			return ((ClusterSourcesRecordList)arg0).getClusterSourcesRecordList().toArray();
		}

	}
	
	/**
	 * This class will be used as a Listener class that will open a dialog box and display the results.
	 */
	class LinkListener {
		
		private TableViewer viewer;
		private TableColumn item;
		
		public LinkListener(TableViewer viewer, TableColumn item) {
			this.viewer = viewer;
			this.item = item;
		}
		
		/**
		 * Creates a link for each column and handles the link click event to which opens a dialog box 
		 * that displays the output result in tabular format
		 * @param parent - Shell
		 * @param table - table to attach the dialog
		 * @param colName - column header of the column the user clicked
		 * @param isRow - To display the entire row data or just the subset of data
		 */
		public void displayLink(final Shell parent, final Table table, final String colName, final String isRow){
			TableViewerColumn actionsNameCol = new TableViewerColumn(viewer, item);
			actionsNameCol.setLabelProvider(new ColumnLabelProvider(){
				@Override
				public void update(final ViewerCell cell) {

					final TableItem item = (TableItem) cell.getItem();
					Link link = new Link((Composite) cell.getViewerRow().getControl(), SWT.NONE);
					if(Constants.ISCOL.equals(isRow)){
						link.setText(Constants.LINK_TEXT);
					}
					
					link.setBackground(Display.getCurrent().getSystemColor(SWT.COLOR_WHITE));
					link.setSize(100, 40);

					TableEditor editor = new TableEditor(item.getParent());
					editor.grabHorizontal = true;
					editor.grabVertical = true;
					editor.horizontalAlignment = SWT.CENTER;
					editor.setEditor(link, item, cell.getColumnIndex());
					editor.layout();
					
					//Add the listener thats displays the dialog 
					link.addListener(SWT.Selection, new Listener() {
						public void handleEvent(Event event) {
							CreatePopUpDialogForClusterSources dlg = new CreatePopUpDialogForClusterSources(parent.getShell());
					        dlg.open(table.indexOf(item), colName, datalist, isRow);
						}
					});
				}
			});
		}//end of displayLink()
		
	}
	
	public static void main(String[] args) {
		String fileName = "C:\\Spoon Demos\\new\\saltdemos\\dataprofile_sub\\ClusterSrc.csv";
		new ClusterSourcesTable(fileName).run();
	}
	
}

package org.hpccsystems.eclguifeatures;

public class RecordBO {
	
	private String columnName 	= "";
	private String defaultValue 	= "";
	private String columnType 	= "";
	private int columnWidth;
	
	public RecordBO(){
		super();
	}
        public RecordBO(String in){
            super();
            fromCSV(in);
        }

	public String getColumnName() {
		return columnName;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	public int getColumnWidth() {
		return columnWidth;
	}

	public void setColumnWidth(int columnWidth) {
		this.columnWidth = columnWidth;
	}
        
        
        
        
        
        
        
        public String toCSV(){
            return columnName + "," + columnType + "," + columnWidth + "," + defaultValue;
        }
        public void fromCSV(String in){
            String[] strArr = in.split("[,]");//"\\,"
            System.out.println("in ---- " + in);
            if(strArr.length == 4){
                columnName = strArr[0];
                columnType = strArr[1];
                columnWidth = Integer.parseInt(strArr[2]);
                defaultValue = strArr[3];

            }else if(strArr.length == 3){
                columnName = strArr[0];
                columnType = strArr[1];
                columnWidth = Integer.parseInt(strArr[2]);
                defaultValue = "";
            }
        }
	
}

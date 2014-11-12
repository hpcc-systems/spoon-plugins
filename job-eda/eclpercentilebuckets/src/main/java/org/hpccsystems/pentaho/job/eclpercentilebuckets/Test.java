package org.hpccsystems.pentaho.job.eclpercentilebuckets;

import org.hpccsystems.recordlayout.RecordBO;
import org.hpccsystems.recordlayout.RecordList;
import java.util.ArrayList;
import java.util.Iterator;

public class Test {
	static RecordList R;
	
	
	
	public static RecordList getR() {
		return R;
	}



	public static void setR(RecordList r) {
		R = r;
	}

	static Integer a;

	public static void main(String[] args) {
		RecordList rec = new RecordList();
		RecordList r = new RecordList();
		System.out.println(rec.getRecords().size());
		RecordBO obj = new RecordBO();
		obj.setColumnName("Me");
		obj.setColumnType("Me");
		obj.setColumnWidth("Me");
		obj.setDefaultValue("Me");
		obj.setSortOrder("Me");
		rec.addRecordBO(obj);
		for(Iterator<RecordBO> it = rec.getRecords().iterator(); it.hasNext();){
			RecordBO ob = (RecordBO) it.next();
			r.addRecordBO(ob);
		}
		
		obj = new RecordBO();
		obj.setColumnName("Me Again");
		obj.setColumnType("Me Again");
		obj.setColumnWidth("Me Again");
		obj.setDefaultValue("Me Again");
		obj.setSortOrder("Me Again");
		//rec.addRecord(0,obj);
		rec.addRecordBO(obj);
		System.out.println(rec.getRecords().size()); 
		ArrayList<RecordBO> ar = new ArrayList<RecordBO>();
		ar = rec.getRecords();
		for(Iterator<RecordBO> it = ar.iterator(); it.hasNext();){
			RecordBO ob = (RecordBO) it.next();
			System.out.println(ob.getColumnName()+"||"+ob.getColumnType()+"||"+ob.getColumnWidth()+"||"+ob.getDefaultValue()+"||"+ob.getSortOrder()); 
		}

		System.out.println(r.getRecords().size());
		System.out.println("-------------------------");
		
		Integer b = 8;
		a = b;
		b = b + 1;
		System.out.println(a + " ---- " + b); 
	}

}

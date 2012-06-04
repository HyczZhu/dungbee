package aStreamingHbaseIncrementalTransaction.transaction;

public class TransactionWrite {
	private String tableName; 
	private byte[] row;
	private byte[] col;
	private byte[] qualifier;
	private byte[] value;
	
	public TransactionWrite()	{
		this(null , null , null , null);
	}
	
	public TransactionWrite(String tableName , byte[] row , byte[] col)	{
		this(tableName , row , col , null);
	}
	
	public TransactionWrite(String tableName , byte[] row , byte[] col , byte[] value)	{
		this.tableName = tableName;
		this.setRow(row);
		this.setCol(col);
		this.value = value;
	}
	
	public void setTableName(String tableName)	{
		this.tableName = tableName;
	}
	
	public void setRow(byte[] row)	{
		this.row = row;
	}
	
	public void setCol(byte[] col)	{
		this.col = col;
	}
	
	public void setQualifier(byte[] qualifier)	{
		this.qualifier = qualifier;
	}
	
	public void setVal(byte[] value)	{
		this.value = value;
	}
	
	public String getTableName()	{
		return tableName;
	}

	public byte[] getCol() {
		return col;
	}

	public byte[] getRow() {
		return row;
	}
	
	public byte[] getQualifier()	{
		return qualifier;
	}
	
	public byte[] getVal()	{
		return value;
	}
	
	/***
	 * 判断当前TransactionWrite是否合法
	 * @return
	 */
	public boolean isValid()	{
		return (tableName != null) && (row != null) && (col != null) && (qualifier != null);
	}
}

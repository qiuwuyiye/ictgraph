package ict.graph.common;

public class ContexInfo {

	private Contex contex;
	
	public ContexInfo(Contex contex) {
		this.contex = contex;
	}
	public ContexInfo(String tempDatabaseName, String tempHdfsBasePath){
		this.contex=new Contex(tempDatabaseName,tempHdfsBasePath);
	}

	public Contex getContex() {
		return contex;
	}
	public void setContex(Contex contex) {
		this.contex = contex;
	}
	
	public class Contex{
		private String tempDatabaseName;
		private String tempHdfsBasePath;
		public Contex(String tempDatabaseName, String tempHdfsBasePath) {
			this.tempDatabaseName = tempDatabaseName;
			this.tempHdfsBasePath = tempHdfsBasePath;
		}
		public String getTempDatabaseName() {
			return tempDatabaseName;
		}
		public void setTempDatabaseName(String tempDatabaseName) {
			this.tempDatabaseName = tempDatabaseName;
		}
		public String getTempHdfsBasePath() {
			return tempHdfsBasePath;
		}
		public void setTempHdfsBasePath(String tempHdfsBasePath) {
			this.tempHdfsBasePath = tempHdfsBasePath;
		}
		
	}
}

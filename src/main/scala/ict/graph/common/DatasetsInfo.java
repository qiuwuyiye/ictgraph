package ict.graph.common;

public class DatasetsInfo {
	private DataSetInfo datasetinfo;
	
	public DatasetsInfo(DataSetInfo datasetinfo) {
		this.datasetinfo = datasetinfo;
	}
	public DatasetsInfo(String inport,String outport,String inport_query,String inport_matrix,String inport_icmodel_seeds){
		this.datasetinfo=new DataSetInfo(inport,outport,inport_query,inport_matrix,inport_icmodel_seeds);
	}
	

	public DataSetInfo getDatasetinfo() {
		return datasetinfo;
	}
	public void setDatasetinfo(DataSetInfo datasetinfo) {
		this.datasetinfo = datasetinfo;
	}


	public class DataSetInfo{
		private String inport;
		private String outport;
		private String inport_query;
		private String inport_matrix;
		private String inport_icmodel_seeds;

		public DataSetInfo(String inport,String outport,String inport_query,String inport_matrix,String inport_icmodel_seeds) {
			this.inport = inport;
			this.outport=outport;
			this.inport_query=inport_query;
			this.inport_matrix=inport_matrix;
			this.inport_icmodel_seeds=inport_icmodel_seeds;
		}

		public String getInport() {
			return inport;
		}

		public String getOutport() {
			return outport;
		}

		public String getInport_query() {
			return inport_query;
		}

		public String getInport_matrix() {
			return inport_matrix;
		}

		public String getInport_icmodel_seeds() {
			return inport_icmodel_seeds;
		}
		
		
	}
}

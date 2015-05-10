package ict.graph.common;

public class ConfigInfo {

	private OperatorInfo operatorInfo;

	private ContexInfo contexInfo;
    private DatasetsInfo datasetsInfo;
    
    private String jobinstanceid;
    
	public void setOperatorInfo(OperatorInfo operatorInfo) {
		this.operatorInfo = operatorInfo;
	}

	public OperatorInfo getOperatorInfo() {
		return this.operatorInfo;
	}

	public DatasetsInfo getDatasetsInfo() {
		return this.datasetsInfo;
	}
	
	public void setDatasetsInfo(DatasetsInfo datasetsInfo) {
		this.datasetsInfo = datasetsInfo;
	}

	public ContexInfo getContexInfo() {
		return contexInfo;
	}

	public void setContexInfo(ContexInfo contexInfo) {
		this.contexInfo = contexInfo;
	}

	public String getJobinstanceid() {
		return jobinstanceid;
	}

	public void setJobinstanceid(String jobinstanceid) {
		this.jobinstanceid = jobinstanceid;
	}
	
	
	
}

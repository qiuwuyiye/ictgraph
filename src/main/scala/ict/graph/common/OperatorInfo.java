package ict.graph.common;


public class OperatorInfo {

	private String operatorName;
	private ParameterInfo parameterInfo;

	public OperatorInfo(String operatorName, ParameterInfo parameterInfo) {
		this.operatorName = operatorName;
		this.parameterInfo = parameterInfo;
	}

	public OperatorInfo(String operatorName, String srcNodeID,
			String desNodeID, String weightString, int iter, int topK,int numrand,int reducedDim,int maxIteration,String inport1,String queryNode) {
		this.operatorName = operatorName;
		this.parameterInfo = new ParameterInfo(srcNodeID, desNodeID,
				weightString, iter, topK,numrand,reducedDim,maxIteration,inport1,queryNode);
	}

	public String getOperatorName() {
		return this.operatorName;
	}

	public void setOperatorName(String operatorName) {
		this.operatorName = operatorName;
	}

	public ParameterInfo getParameterInfo() {
		return this.parameterInfo;
	}

	// public void setParameterInfo(ParameterInfo parameterInfo) {
	// this.parameterInfo = parameterInfo;
	// }

	public class ParameterInfo {
		private String srcNodeID;
		private String desNodeID;
		private String weightString;
		private String inport1;
		private String queryNode;
		private int iter;
		private int topK;
		private int numrand;
		private int reducedDim;
		private int maxIteration;

		public ParameterInfo(String srcNodeID, String desNodeID,
				String weightString, int iter, int topK,int numrand,int reducedDim,int maxIteration,String inport1,String queryNode) {
			this.srcNodeID = srcNodeID;
			this.desNodeID = desNodeID;
			this.weightString = weightString;
			this.iter = iter;
			this.topK = topK;
			this.numrand=numrand;
			this.reducedDim=reducedDim;
			this.maxIteration=maxIteration;
			this.inport1=inport1;
			this.queryNode=queryNode;
		}

		public void setSrcNodeID(String srcNodeID) {
			this.srcNodeID = srcNodeID;
		}

		public String getSrcNodeID() {
			return this.srcNodeID;
		}

		public void setDesNodeID(String desNodeID) {
			this.desNodeID = desNodeID;
		}

		public String getDesNodeID() {
			return this.desNodeID;
		}

		public void setWeightString(String weightString) {
			this.weightString = weightString;
		}

		public String getWeightString() {
			return this.weightString;
		}

		public void setIter(int iter) {
			this.iter = iter;
		}

		public int getIter() {
			return this.iter;
		}

		public void setTopK(int topK) {
			this.topK = topK;
		}

		public int getTopK() {
			return this.topK;
		}

		public int getNumrand() {
			return numrand;
		}

		public void setNumrand(int numrand) {
			this.numrand = numrand;
		}

		public int getReducedDim() {
			return reducedDim;
		}

		public void setReducedDim(int reducedDim) {
			this.reducedDim = reducedDim;
		}

		public int getMaxIteration() {
			return maxIteration;
		}

		public void setMaxIteration(int maxIteration) {
			this.maxIteration = maxIteration;
		}
		public String getqueryNode() {
			return queryNode;
		}
		public void setqueryNode(String queryNode) {
			this.queryNode = queryNode;
		}
		public String getInport1() {
			return inport1;
		}

		public void setInport1(String inport1) {
			this.inport1 = inport1;
		}
		
	}
}

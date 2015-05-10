package ict.graph.common;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class ConfigReader {

	private String xmlFilePath;

	public ConfigReader(String xmlpathString) {
		xmlFilePath = xmlpathString;
	}

	public ConfigInfo ReadConfig() {

		ConfigInfo retConfigInfo = null;
		try {
			Path pt=new Path(xmlFilePath);
            FileSystem fs = FileSystem.get(new Configuration());
			SAXReader reader = new SAXReader();
			Document doc = reader.read(fs.open(pt));
			retConfigInfo = new ConfigInfo();

			OperatorInfo operatorInfo = readOperatorInfo(doc);
			if (operatorInfo == null) {
				return null;
			}
			retConfigInfo.setOperatorInfo(operatorInfo);
			
			DatasetsInfo datasetsinfo=readDatasetsInfo(doc);
			if(datasetsinfo==null){
				return null;
			}
			retConfigInfo.setDatasetsInfo(datasetsinfo);
			
			ContexInfo contexInfo=readContexInfo(doc);
			if(contexInfo==null){
				return null;
			}	
			retConfigInfo.setContexInfo(contexInfo);
			
			Element jobidElement = (Element) doc.selectSingleNode("//jobinstanceid");
			String jobinstanceid=jobidElement.getText();
			retConfigInfo.setJobinstanceid(jobinstanceid);
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return null;
		}

		return retConfigInfo;
	}

	private OperatorInfo readOperatorInfo(Document doc) {

		Element operatorElement = (Element) doc.selectSingleNode("//operator");
		if (null == operatorElement) {
			return null;
		}
		String operatorName = operatorElement.attributeValue("name");

		if (operatorName == null) {
			return null;
		}

		List<Element> paraElements = doc.selectNodes("//operator/parameter");
		if (null == paraElements) {
			return null;
		}
		String srcNodeID = null;
		String desNodeID = null;
		String weight = null;
		String inport1=null;
		int reducedDim=0;
		int maxIteration=10;
		int iter = -1;
		int topK = -1;
		int numrand=-1;
		String queryNode=null;
		for (Element elem : paraElements) {
			String nameValue = elem.attributeValue("name");
			if (nameValue.equals("srcNodeID"))
				srcNodeID = elem.getText();
			else if (nameValue.equals("desNodeID"))
				desNodeID = elem.getText();
			else if (nameValue.equals("weight"))
				weight = elem.getText();
			else if (nameValue.equals("maxiterNumber"))
				iter = Integer.parseInt(elem.getText());
			else if (nameValue.equals("topK"))
				topK = Integer.parseInt(elem.getText());
			else if (nameValue.equals("numrand"))
				numrand = Integer.parseInt(elem.getText());
			else if (nameValue.equals("reducedDim"))
				reducedDim = Integer.parseInt(elem.getText());
			else if (nameValue.equals("maxIteration"))
				maxIteration = Integer.parseInt(elem.getText());
			else if (nameValue.equals("inputTableName1"))
				inport1 = elem.getText();
			else if (nameValue.equals("queryNode"))
				queryNode = elem.getText();

		}
		return new OperatorInfo(operatorName, srcNodeID, desNodeID, weight,
				iter, topK,numrand,reducedDim,maxIteration,inport1,queryNode);
	}
	private DatasetsInfo readDatasetsInfo(Document doc){
		List<Element> paraElements=doc.selectNodes("//datasets/dataset");
		if(null==paraElements){
			return null;
		}
		String inport="ictgraph.edges";
		String outport="ictgraph.pagerank";
		String inport_query="ictgraph.edges";
		String inport_matrix="ictgraph.edges";
		String inport_icmodel_seeds="ictgraph.edges";
		for(Element elem:paraElements){
			String nameValue=elem.attributeValue("name");
			if(nameValue.equals("inport1")){
				Element rowElem = (Element) elem.selectSingleNode("row");
				inport=rowElem.getText();   
			}
			if(nameValue.equals("outport")){
				Element rowElem = (Element) elem.selectSingleNode("row");
				outport=rowElem.getText();
			}
			if(nameValue.equals("inport2")){
				Element rowElem = (Element) elem.selectSingleNode("row");
				inport_query=rowElem.getText();   
			}
			if(nameValue.equals("inport3")){
				Element rowElem = (Element) elem.selectSingleNode("row");
				inport_matrix=rowElem.getText();   
			}
			if(nameValue.equals("inport2")){
				Element rowElem = (Element) elem.selectSingleNode("row");
				inport_icmodel_seeds=rowElem.getText();   
			}
				
		}
		return new DatasetsInfo(inport,outport,inport_query,inport_matrix,inport_icmodel_seeds);
	}
	
	private ContexInfo readContexInfo(Document doc){
		List<Element> contextElements=doc.selectNodes("//context/property");
		if(null==contextElements){
			return null;
		}
		String tempDatabaseName=null;
		String tempHdfsBasePath=null;
		for(Element elem:contextElements){
			String nameValue = elem.attributeValue("name");
			if(nameValue.equals("tempDatabaseName")){
				tempDatabaseName=elem.attributeValue("value");
			}
			else if(nameValue.equals("tempHdfsBasePath")){
				tempHdfsBasePath=elem.attributeValue("value");
			}	
		}
		return new ContexInfo(tempDatabaseName,tempHdfsBasePath);
	}
	
}

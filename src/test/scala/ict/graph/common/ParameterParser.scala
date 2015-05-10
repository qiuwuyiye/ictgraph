package ict.graph.common

object ParameterParser {

  def main(args: Array[String]): Unit = {

    val configFile: String = "description.xml"
    val configReader = new ConfigReader(configFile)
    val configInfo = configReader.ReadConfig();

    println("operator name:\t" + configInfo.getOperatorInfo().getOperatorName())
    println("parameter srcNodeID:\t" + configInfo.getOperatorInfo().getParameterInfo().getSrcNodeID())
    println("parameter desNodeID:\t" + configInfo.getOperatorInfo().getParameterInfo().getDesNodeID())

    println("parameter weight:\t" + configInfo.getOperatorInfo().getParameterInfo().getWeightString())
    println("parameter iter:\t" + configInfo.getOperatorInfo().getParameterInfo().getIter())
    println("parameter topK:\t" + configInfo.getOperatorInfo().getParameterInfo().getTopK())
    val in_tb=configInfo.getDatasetsInfo().getDatasetinfo().getInport().trim().split("\\.")
    val out_tb=configInfo.getDatasetsInfo().getDatasetinfo().getOutport().trim().split("\\.")
    val in_tb_query=configInfo.getDatasetsInfo().getDatasetinfo().getInport_query().trim().split("\\.")
    val in_tb_matrix=configInfo.getDatasetsInfo().getDatasetinfo().getInport_matrix().trim().split("\\.")
    
    val in_tb_inport_icmodel_seeds=configInfo.getDatasetsInfo().getDatasetinfo().getInport_icmodel_seeds().trim().split("\\.")
    
    val reducedDim=configInfo.getOperatorInfo().getParameterInfo().getReducedDim();
    val maxIteration=configInfo.getOperatorInfo().getParameterInfo().getMaxIteration();
    
    println("parameter in_dbname:\t"+in_tb.apply(0))
    println("parameter in_tbname:\t"+in_tb.apply(1))
    
    println("parameter out_dbname:\t"+out_tb.apply(0))
    println("parameter out_tbname:\t"+out_tb.apply(1))
    
    println("parameter in_tb_queryname:\t"+in_tb_query.apply(0))
    println("parameter in_tb_queryname:\t"+in_tb_query.apply(1))
    
    println("parameter in_tb_queryname:\t"+in_tb_inport_icmodel_seeds.apply(0))
    println("parameter in_tb_queryname:\t"+in_tb_inport_icmodel_seeds.apply(1))
    
    println("parameter reducedDim:\t" + reducedDim.toString)
    
    println("parameter maxIteration:\t" + maxIteration.toString)
    
    println("property tempDatabaseName:\t"+configInfo.getContexInfo().getContex().getTempDatabaseName())
    println("property tempHdfsBasePath:\t"+configInfo.getContexInfo().getContex().getTempHdfsBasePath())
    
    println("jobinstanceid:\t"+configInfo.getJobinstanceid())
    
    println("parameter inputTableName1:\t" + configInfo.getOperatorInfo().getParameterInfo().getInport1())
    
    
    
    
  }

}
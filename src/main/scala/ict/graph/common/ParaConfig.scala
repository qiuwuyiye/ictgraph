package ict.graph.common

case class ParaConfig(
  edgesfile: String = "hdfs://s1:8020/user/root/caoqi/CorePeople/CorePeopleEdges.txt",
  vertexfile: String = "hdfs://s1:8020/user/root/caoqi/CorePeople/filterVertex.txt",
  seedfile: String = "seeds.txt",
  outpath: String = "hdfs://s1:8020//user/root/caoqi/CorePeople/output/InDegree",
  output: String = null,
  topk: Int = 10,
  startnode: Long = 1,
  partitions: Int = 2,
  queryfile:String = null,
  local: Boolean = false,
  seq:Boolean = false,
  numrand: Int = 1000,
  Dfile:String=null,
  //pagerank_input: String = "hdfs://s1:8020/user/root/input/edges.txt",
  //pagerank_output: String = "hdfs://s1:8020/user/root/output/pagerank",
  reducedDim: Int = 3,
  R:Int = 100,
  WorH:Int = 0,
  maxIteration: Int = 100,
  c:String="hdfs://bde25:8020/user/liping/ictgraph/edges.txt",
  theta: Double = 0.01,
  lambda: Double = 0.1,
  withZeroItems: Int = 0)
  
case class SimRankPara(
  dbName:String=null,
  edgesTb:String=null,
  verticesTb:String=null,
  outputTb:String=null,
  iterationT:Int=5,//simrank的迭代次数
  partitions: Int = 2,
  dTb:String=null,
  Dfile:String=null,
  queryTb:String=null,
  C:Double=0.8,//simrank的衰减系数，默认为0.8
  L:Int=5,//simrank估计D时的迭代次数，默认为5
  sampleR:Int=1000,//simrank估计D时的随机样本数量，默认为100
  k:Int=1
  
    )
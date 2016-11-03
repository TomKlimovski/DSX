
/*
* ====OLETypes===== (there should be more, this is just for TX_EBOS_ACCOUNT_VEP)
* CTrxOutput, 				-> Transformer Output Information, link-name, column names and long-form column metadata
* CCustomInput, 			-> Input stages; usually datasets
* CCustomOutput, 			-> Links
* CCustomStage, 			-> Refer to StageType field for the Stage, e.g. StageType "PxLookup", StageType "PxDataSet"
* CContainerStage			-> Container
* CTrxInput, 					-> Transformer input
* CTransformerStage,  -> Transformer Stage
* CStdPin, CJobDefn,
* CContainerView, 		-> Contains a nifty, pipe-delimited overview of the canvas
* CAnnotat						-> The annotations that exist
*/

	case class Boundary(start: Int, end: Int)
	case class File(line: String, lineNum: Int)

	class helpers {
		def cleanJobName(job: (String, Int)) = job._1.split("\"").last
	  def processDSX_File(file: Array[(String, Int)]): Map[String, (Int, Int)] = {
  		val beginDsJobs = file.filter(_._1== "BEGIN DSJOB").map(_._2).toSeq.sorted
  		val endDsJobs = file.filter(_._1 == "END DSJOB").map(_._2).toSeq.sorted
  	  val jobsStartFinish = beginDsJobs zip endDsJobs
  	  jobsStartFinish.map{ case(a, b) => (cleanJobName(helper.lines(a+1)) -> (a, b)) }.toMap
  	}
	}
	class DsSubRecord(file: Array[(String, Int)], jobInfo: Map[String, (Int, Int)], boundary: Boundary) extends helpers {
		/*
		*tech.debt -> this isn't the exhaustive list of names in a record; it can vary
		*/
    val Name: String = cleanJobName(file(boundary.start+1))
    val Description: String = cleanJobName(file(boundary.start+2))
	}
	class DsRecord(file: Array[(String, Int)], jobInfo: Map[String, (Int, Int)], boundary: Boundary) extends helpers {
		/*
		*tech.debt -> this isn't the exhaustive list of names in a record; it can vary
		*/
	  def SubRecordInfo: Map[String, (Int, Int)] = {
  		val beginSubDsRecords = file.filter(x => (x._1).trim == "BEGIN DSSUBRECORD" && x._2 >= boundary.start && x._2 <= boundary.end).map(_._2).toSeq.sorted
  		val endDsSubRecords = file.filter(x => (x._1).trim == "END DSSUBRECORD" && x._2 >= boundary.start && x._2 <= boundary.end).map(_._2).toSeq.sorted
  	  val subRecordsStartFinish = beginSubDsRecords zip endDsSubRecords
  	  subRecordsStartFinish.map{ case(a, b) => (cleanJobName(helper.lines(a+1)) -> (a, b)) }.toMap
  	}
    val   Identifier: String = cleanJobName(file(boundary.start+1))
    val   OLEType: String = cleanJobName(file(boundary.start+2))
    val   Readonly: String = cleanJobName(file(boundary.start+3))
    val   Name: String = cleanJobName(file(boundary.start+4))
    val   NextID: String = cleanJobName(file(boundary.start+5))
    val   InputPins: String = cleanJobName(file(boundary.start+6))
    val   ParameterValues: String = cleanJobName(file(boundary.start+7))

		def dsSubRecords: List[DsSubRecord] = {
	  		  	(for((subRecordName, (start, end)) <- SubRecordInfo)
	  		  	yield {new DsSubRecord(file, jobInfo, Boundary(start, end))}).toList
	  	}
	  def printSubRecords = {dsSubRecords.foreach(x=>println("Name: " + x.Name + " Description: " + x.Description))}
	}


	class DsJobs(file: Array[(String, Int)], jobInfo: Map[String, (Int, Int)], boundary: Boundary) extends helpers {

	  def recordInfo: Map[String, (Int, Int)] = {
  		val beginDsRecords = file.filter(x => (x._1).trim == "BEGIN DSRECORD" && x._2 >= boundary.start && x._2 <= boundary.end).map(_._2).toSeq.sorted
  		val endDsRecords = file.filter(x => (x._1).trim == "END DSRECORD" && x._2 >= boundary.start && x._2 <= boundary.end).map(_._2).toSeq.sorted
  	  val recordsStartFinish = beginDsRecords zip endDsRecords
  	  recordsStartFinish.map{ case(a, b) => (cleanJobName(helper.lines(a+1)) -> (a, b)) }.toMap
  	}

		lazy val Identifier: String = cleanJobName(file(boundary.start+1))
		lazy val DateModified: String = cleanJobName(file(boundary.start+2))
		lazy val TimeModified: String = cleanJobName(file(boundary.start+3))

	  def dsRecords: List[DsRecord] = {
	  		  	(for((recordName, (start, end)) <- recordInfo)
	  		  	yield {new DsRecord(file, jobInfo, Boundary(start, end))}).toList
	  	}
	  def printRecords = {dsRecords.foreach(x=>println(x.Identifier))}

		def getStageNames: String = file.filter(x => (x._1).trim.startsWith("StageNames") && x._2 >= boundary.start && x._2 <= boundary.end).map(_._1).mkString("")
		def getStageTypes: String = file.filter(x => (x._1).trim.startsWith("StageTypes") && x._2 >= boundary.start && x._2 <= boundary.end).map(_._1).mkString("")
		def getStageInfoLine(stage: String) = file.filter(x => (x._1).trim.startsWith("#### STAGE: "+stage) && x._2 >= boundary.start && x._2 <= boundary.end).map(_._2).mkString
		def getStageInfoLines(stage: String) = {
			val filtered = file.filter(x => (x._1).trim.startsWith("#### STAGE: "+stage) && x._2 >= boundary.start && x._2 <= boundary.end).map(_._2)
			//filtered.map(x => println("Filtered: " + x))
			val results =for{line <- filtered
				if(file(line-1)._1.contains("###################"))
				 } yield(line)
			//results.map(x => println("LINEresultss: " + x))
			results.head
		}
		def getOutput(lineNum: Int) = {
			file.filter(x => (x._1).trim.startsWith("0>") && x._2 > lineNum && x._2 < lineNum + 15).map(_._1).mkString("")
		}
	}

	class DS(file: Array[(String, Int)]) extends helpers{
		val CharacterSet = cleanJobName(file(1))
   	val ExportingTool: String = cleanJobName(file(2))
    val ToolVersion: String = cleanJobName(file(3))
    val ServerName: String = cleanJobName(file(4))
    val ToolInstanceID: String = cleanJobName(file(5))
    val MDISVersion: String = cleanJobName(file(6))
    val Date: String = cleanJobName(file(7))
    val Time: String = cleanJobName(file(8))
    val ServerVersion: String = cleanJobName(file(9))
    val jobInfo = processDSX_File(file)

		def dsJobs: List[DsJobs] ={
			(for((jobName, (start, end)) <- jobInfo) yield {new DsJobs(file, jobInfo, Boundary(start, end))}).toList
		}
		lazy val printJobs = {dsJobs.foreach(x=>println(x.Identifier))}

		lazy val printLinks = {
			val results = dsJobs.map{x => x.dsRecords.map{z =>
								if((z.OLEType == "CCustomInput" || z.OLEType == "CCustomOutput") && x.Identifier =="TX_EBOS_ACCOUNT_VEP") println("DsRecord: " + z.Name + " DsJob: " + x.Identifier)}}
		}

		lazy val printTransformers = {
			val results = dsJobs.map{x => x.dsRecords.map{z =>
								if((z.OLEType == "CTransformerStage") && x.Identifier =="TX_EBOS_ACCOUNT_VEP") println("DsRecord: " + z.Name + " DsJob: " + x.Identifier)}}

	  }
		lazy val printDatasets = {
			val results = dsJobs.map{x => x.dsRecords.map{z => z.dsSubRecords.map{d =>
					if(d.Name == "dataset" && x.Identifier =="TX_EBOS_ACCOUNT_VEP") println("DsSubRecord: " + d.Description + " DsRecord: " + z.Name + " DsJob: " + x.Identifier)}}}

	  }
	  lazy val getOLETypes: List[String] = {
			val results = dsJobs.flatMap{x => x.dsRecords.map{z => z.OLEType.trim}}
			results.distinct
	  }
		lazy val getStageNames = {
	 			val outputs = {dsJobs.map{x =>
						if(x.Identifier =="TX_EBOS_ACCOUNT_VEP") x.getStageNames}}.mkString
	 			val testies = {dsJobs.map{x => x.getStageNames}}.mkString
	 			//println("TESTIES: " + testies)
				outputs
		}
		lazy val getStageTypes = {
	 			{dsJobs.map{x =>
						if(x.Identifier =="TX_EBOS_ACCOUNT_VEP") x.getStageTypes}}.mkString
		}
		def getStageInfo(stage: String) = {
			val infoLineNum = (for{jobs <- dsJobs
					if(jobs.Identifier =="TX_EBOS_ACCOUNT_VEP")
				 } yield (jobs.getStageInfoLines(stage))).mkString

			val result = for{jobs <- dsJobs
					if(jobs.Identifier =="TX_EBOS_ACCOUNT_VEP")
				 } yield (jobs.getOutput(infoLineNum.toInt))
			//println("Result getStageInfo: " + result)
			result
		}
		def getStages: Map[String, Iterable[String]] = {
			val stageNames = LinkedHashSet(getStageNames.split("\"")(1).trim.split("\\|"))
			val stageTypes = LinkedHashSet(getStageTypes.split("\"")(1).trim.split("\\|"))
			val stageZip = stageNames zip stageTypes
			val result = stageZip.map{case (a, b) => (a zip b).toMap}

			result.head.groupBy(_._2).mapValues(_.keys)
		}
		def findTargetTypes ={
			val ccustomStageMap = getStages getOrElse("CCustomStage", Set.empty)
			//println("getStageInfo: " + ccustomStageMap)
			val targets = for{stage <- ccustomStageMap
					if(getStageInfo(stage).forall(x => x.contains("[ds]")))
					} yield {(stage)}
			//targets.foreach(x => println(x + " is a target stage"))
			//println("PRINTS-SIZE: " + targets.size)
			targets
		}
	}

	class DSX(file: Array[(String, Int)]) extends helpers {
		val ds = new DS(file)

		//override def toString = "Identifier: " + ds.jobs.Identifier
	}

  object helper {
	  val Path = "/Users/tomklimovski/Google Drive/EPAM/DSX/"
	  val fileName = "Mini_Project.dsx"
	  val source = scala.io.Source.fromFile(Path + fileName)
	  val lines: Array[(String, Int)] = (try source.getLines mkString "\n"  finally source.close()).split("\n").zipWithIndex

  }
//StageTypes "ID_PALETTEJOBANNOTATION|CTransformerStage|CCustomStage|CCustomStage|CTransformerStage|CCustomStage|CCustomStage|CCustomStage|ID_PALETTEANNOTATION|ID_PALETTEPXSHAREDCONTAINER|CCustomStage|CCustomStage|CCustomStage|CCustomStage"
//StageNames " |xfmPreTransform|filterCB|lkpClosingBalance|xfmMapping|tgt_dsStgAccountVep|src_dsCasAccount|dsCasTxDetail| |scNullBalance|FilterEndTime|dsCasAccountStatusLog|dsCasVehicle|rdupVehEndDate"

/*
* 1.1 Try and identify output tables and input tables
* 1.2 Once 1.1 is complete, try and identify driving tables versus reference tables
* 2.1 Try and do some data lineage
*/

  val newDSX = new DSX((helper.lines))            //> newDSX  : chapter15.DSX.DSX = chapter15.DSX$$anonfun$main$1$DSX$1@685cb137
                                                  //|
  //println(newDSX)
  //newDSX.ds.dsJobs(0).dsRecords(0).printSubRecords
  //newDSX.ds.printBreadCrumb("CCustomInput")
  //val stageTypes = (newDSX.ds.getStageTypes)

  val canvas = (newDSX.ds.findTargetTypes)        //> canvas  : Iterable[String] = Set(tgt_dsStgAccountVep)
  //canvas.foreach(println)

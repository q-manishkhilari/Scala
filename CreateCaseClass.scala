// Data Engineer Technical Academy 
// January 2023 
// This is progress while the Virtual Development Environment is down 

package com.quantexa.academytask.etl.projects.icij 

import com.quantexa.academytask.model.icij.IcijRawModel.{ImportedAddress, ImportedEdge, ImportedIntermediary, ImportedOfficer, ImportedOffshore} 

// Stage 3 
// Reading Raw Parquet to Clean Case Classes 

object CreateCaseClass extends  TypedSparkScriptIncremental[DocumentConfig[IcijInputFiles]] {

  val name = "CreateCaseClass - icij"

  val fileDependencies = Map.empty[String, String]

  val scriptDependencies = Set.empty[QuantexaSparkScript]

  def run(spark: SparkSession, logger: Logger, args: Seq[String],
          projectConfig: DocumentConfig[IcijInputFiles],
          etlMetricsRepository: ETLMetricsRepository, 
          metadataRunId: MetadataRunId): Unit = {
    
    if (args.nonEmpty) {
      logger.warn(args.length + " arguments were passed to the script and are being ignored")
    }

    import spark.implicits._
    
    val parquetRoot = projectConfig.parquetRoot 
    
    val addressInputParquetFile = "/panama.address.parquet" 
    val intermediaryInputParquetFile = "/panama.intermediary.parquet" 
    val edgeInputParquetFile = "/panama.edge.parquet" 
    val officerInputParquetFile = "/panama.officer.parquet" 
    val offshoreInputParquetFile = "/panama.offshore.parquet" 
    
    val addressInputParquetPath = s"$parquetRoot $addressInputParquetFile" 
    val intermediaryInputParquetPath = s"$parquetRoot $intermediaryInputParquetFile" 
    val edgeInputParquetPath = s"$parquetRoot $edgeInputParquetFile" 
    val officerInputParquetPath = s"$parquetRoot $officerInputParquetFile" 
    val offshoreInputParquetPath = s"$parquetRoot $offshoreInputParquetFile" 

    val addressRawDataset = spark.read.parquet(addressInputParquet).as[ImportedAddress] 
    val edgesRawDataset = spark.read.parquet(edgesInputParquet).as[ImportedEdge] 
    val intermediaryRawDataset = spark.read.parquet(intermediaryInputParquet).as[ImportedIntermediary] 
    val officerRawDataset = spark.read.parquet(officerInputParquet).as[ImportedOfficer] 
    val offshoreRawDataset = spark.read.parquet(offshoreInputParquet).as[ImportedOffshore] 
    
    val intermediaryDataset : Dataset[Intermediary] 
    = intermediaryRawDataset.map(
      a => Intermediary(
      
      ) 
    ) 
    
     val officerDataset : Dataset[Officer] 
    = officerRawDataset.map(
      a => Officer(
      
      ) 
    ) 
    
    val offshoreDataset : Dataset[Offshore] 
    = offshoreRawDataset.map(
      a => Offshore(
        offshoreId = a.node_id, 
        businessName = a.name, 
        jurisdiction = a.jurisdiction, 
        jurisdictionDescription = a.jurisdiction_description, 
        countries = a.countries, 
        countryCodes = a.country_codes, 
        incorporationDate = toDate(a.incorporation_date), 
        inactivationDate = toDate(a.inactivation_date), 
        closedDate = toDate(a.closed_date), 
        struckOffDate = toDate(a.struck_off_date), 
        ibcRUC = a.ibcRUC, 
        status = a.status, 
        companyType = a.company_type, 
        serviceProvider = a.service_provider, 
        sourceId = a.sourceID, 
        validUntil = a.valid_until, 
        note = a.note 
      ) 
    ) 
    
    val destinationPath = projectConfig.caseClassPath 
    
    
}

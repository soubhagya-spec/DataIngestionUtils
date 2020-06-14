#Package

ReadFileFromSource(spark,cnx,entryid,jdbcUrl,connectionProperties) -> BronzeDf,MetadataDf
DataqualityCheck(BronzeDf,MetadataDf) -> DF,metadata_dict
typecast(spark,DF,metadata_dict) -> SilverDf,RejectedDf
WriteFileToTarget(spark,cnx,entryid,jdbcUrl,connectionProperties,SilverDf,RejectedDf)
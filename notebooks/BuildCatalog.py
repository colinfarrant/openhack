# Databricks notebook source
from pyspark.sql.functions import explode

# COMMAND ----------

#Southridge Movies
srm = spark.read.json("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/CosmosDBInput/CosmosDB.json")
#display(srm)

# COMMAND ----------

srm.createOrReplaceTempView("SVmoviesRaw")

# COMMAND ----------



# COMMAND ----------

# Add a unique ID
srm_unique=spark.sql("select 1 as SourceID,  uuid() as CatalogId, id as SourceSystemMovieId, id as SouthridgeMovieId, actors, to_date(AvailabilityDate) as ReleaseDate , genre as Genre, rating as Rating, cast(releaseYear as int) as AvailabilityYear,  to_date(streamingAvailabilityDate) as AvailabilityDate, cast(tier as int) as MovieTier, title as MovieTitle ,id as MovieID,cast(runtime as int) as RuntimeMinutes  from SVmoviesRaw")

# COMMAND ----------

#Explode out actor names
srm_exploded = srm_unique.withColumn('actors', explode('actors'))
display(srm_exploded)

# COMMAND ----------

srm_exploded.createOrReplaceTempView("SVmovies")


# COMMAND ----------

finalSouthridge=spark.sql("select SourceID,  CatalogID, '' as ActorID, actors.name as ActorName, ReleaseDate,Genre, Rating, AvailabilityYear, AvailabilityDate, MovieTier, MovieTitle, MovieID  from SVmovies")

# COMMAND ----------

finalSouthridge.createOrReplaceTempView("SRFinal")

# COMMAND ----------

# MAGIC %sql select * from SV_Out

# COMMAND ----------

#FourthCoffeeSource Movies, Actors, MovieActors, OnlineMovieMappings
fcm = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/FolderFourthCoffeeInput/Movies.csv", header=True)
fca = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/FolderFourthCoffeeInput/Actors.csv", header=True)
fcma = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/FolderFourthCoffeeInput/MovieActors.csv", header=True)
fcomm = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/FolderFourthCoffeeInput/OnlineMovieMappings.csv", header=True)

#display(srm)

# COMMAND ----------

fcm.createOrReplaceTempView("FCMoviesRaw")
fca.createOrReplaceTempView("FCActors")
fcma.createOrReplaceTempView("FCMovieActorMapping")
fcomm.createOrReplaceTempView("FCOnlineMovieMappings")

# COMMAND ----------

# Add a unique ID
fcmm_unique=spark.sql("select 3 as SourceID,  uuid() as CatalogID,  Category as Genre, Rating as Rating, to_date(concat(right(ReleaseDate,4),'-',left(ReleaseDate,5))) as AvailabilityDate,  null as MovieTier, FCMoviesRaw.MovieTitle as MovieTitle, coalesce(OnlineMovieID,   FCMoviesRaw.MovieID) as MovieID   from FCMoviesRaw left join FCOnlineMovieMappings on FCMoviesRaw.MovieID = FCOnlineMovieMappings.MovieID")

# COMMAND ----------

fcmm_unique.createOrReplaceTempView("FCMovies")

# COMMAND ----------

#fcactormovies=spark.sql("select FCMovies.MovieID, FCMovieActorMapping.ActorID, FCActors.ActorName from FCMovies inner join FCMovieActorMapping on FCMovies.MovieID = FCMovieActorMapping.MovieID inner join FCActors on FCActors.ActorID = FCMovieActorMapping.ActorID")

# COMMAND ----------

#fcactormovies.createOrReplaceTempView("FCActorMovies")

# COMMAND ----------

finalFirstCoffee=spark.sql("select SourceID, CatalogID, FCActors.ActorID, ActorName as Actor, AvailabilityDate as ReleaseDate, Genre, Rating, Year(AvailabilityDate) as AvailibilityYear, AvailabilityDate,  MovieTier,MovieTitle, FCMovies.MovieID from FCMovies inner join FCMovieActorMapping on FCMovies.MovieID = FCMovieActorMapping.MovieID inner join FCActors on FCActors.ActorID = FCMovieActorMapping.ActorID")

# COMMAND ----------

finalFirstCoffee.createOrReplaceTempView("FCFinal")

# COMMAND ----------

#Vanardel Movies, Actors, MovieActors, OnlineMovieMappings
vam = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLVanArsdellInput/Movies.txt", header=True)
vaa = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLVanArsdellInput/Actors.txt", header=True)
vama = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLVanArsdellInput/MovieActors.txt", header=True)
vaomm = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLVanArsdellInput/OnlineMovieMappings.txt", header=True)


# COMMAND ----------

vam.createOrReplaceTempView("VAMoviesRaw")
vaa.createOrReplaceTempView("VAActors")
vama.createOrReplaceTempView("VAMovieActorMapping")
vaomm.createOrReplaceTempView("VAOnlineMovieMappings")

# COMMAND ----------

# Add a unique ID
vamm_unique=spark.sql("select 2 as SourceID,  uuid() as CatalogID,  Category as Genre, Rating as Rating, to_date(concat(right(ReleaseDate,4),'-',left(ReleaseDate,5))) as AvailabilityDate,  null as MovieTier, VAMoviesRaw.MovieTitle as MovieTitle, coalesce(OnlineMovieID,   VAMoviesRaw.MovieID) as MovieID   from VAMoviesRaw left join VAOnlineMovieMappings on VAMoviesRaw.MovieID = VAOnlineMovieMappings.MovieID")

# COMMAND ----------

vamm_unique.createOrReplaceTempView("VAMovies")

# COMMAND ----------

finalVanArsdel=spark.sql("select SourceID, CatalogID, VAActors.ActorID, ActorName as Actor, AvailabilityDate as ReleaseDate, Genre, Rating, Year(AvailabilityDate) as AvailibilityYear, AvailabilityDate,  MovieTier,MovieTitle, VAMovies.MovieID from VAMovies inner join VAMovieActorMapping on VAMovies.MovieID = VAMovieActorMapping.MovieID inner join VAActors on VAActors.ActorID = VAMovieActorMapping.ActorID")

# COMMAND ----------

finalVanArsdel.createOrReplaceTempView("VAFinal")

# COMMAND ----------

# MAGIC %sql select * from VAFinal union select * from FCFinal union select * from SRFinal

# COMMAND ----------

# MAGIC %sql select SourceID, CatalogID, FCActors.ActorID, ActorName as Actor, AvailabilityDate as ReleaseDate, Genre, Rating, Year(AvailabilityDate) as AvailibilityYear, AvailabilityDate,  MovieTier,MovieTitle, FCMovies.MovieID from FCMovies inner join FCMovieActorMapping on FCMovies.MovieID = FCMovieActorMapping.MovieID inner join FCActors on FCActors.ActorID = FCMovieActorMapping.ActorID

# COMMAND ----------

fcm_unique=spark.sql("select 2 as SourceSystemId,  uuid() as CatalogId, MovieID as SourceSystemMovieId, 222 as SouthridgeMovieId, actors,  MovieTitle as Title, Category as Genre, Rating as Rating, cast(RunTimeMin as int) as RuntimeMinutes, cast(releaseYear as int) as TheatricalReleaseYear, to_date(ReleaseDate) as PhysicalAvailabilityDate,   from FCMoviesRaw")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql select count(*) from (select distinct FCMovies.MovieID, FCActors.ActorID, FCActors.ActorName, FCActors.Gender from FCMovies
# MAGIC inner join FCMovieActors on FCMovies.MovieID = FCMovieActors.MovieID 
# MAGIC inner join FCActors on FCActors.ActorID = FCMovieActors.ActorID)

# COMMAND ----------

# MAGIC %sql select distinct count(*) from 

# COMMAND ----------

# MAGIC %sql select count(*) from FCMovies where not exists (select * from FCMovieActors where FCMovieActors.MovieID=FCMovies.MovieID )

# COMMAND ----------

# MAGIC %sql select count(*) from FCMovieActors where not exists (select * from FCMovies where FCMovieActors.MovieID=FCMovies.MovieID )

# COMMAND ----------

# MAGIC %sql select count(*) from FCMovies where not exists (select * from FCMovieActors where FCMovieActors.MovieID=FCMovies.MovieID )

# COMMAND ----------

# MAGIC %sql select 1 as SourceSystemId,  uuid() as CatalogId, id as SourceSystemMovieId, id as SouthridgeMovieId, actors as ActorName, ' ' as ActorGender, 
# MAGIC title as Title, genre as Genre, rating as Rating, runtime as RuntimeMinutes, releaseYear as TheatricalReleaseYear, availabilityDate as PhysicalAvailabilityDate, streamingAvailabilityDate as  StreamingAvailabilityDate  from SVmovies 

# COMMAND ----------

srm_exploded = srm.withColumn('actors', explode('actors'))
display(srm_exploded)

# COMMAND ----------

srm_exploded.createOrReplaceTempView("SVmovies")

# COMMAND ----------

#Get unique
%sql select distinct uuid() as CatalogId, id as SourceSystemMovieId, id as SouthridgeMovieId 
newts=spark.sql("select distinct uuid() as CatalogId, id as SourceSystemMovieId, id as SouthridgeMovieId")

# COMMAND ----------

# MAGIC %sql select 1 as SourceSystemId,  uuid() as CatalogId
# MAGIC ,_attachments, _etag, _rid, _self, _ts, actors.name,availabilityDate, genre,id, rating releaseYear, runtime, streamingAvailabilityDate, tier, title from SVmovies 

# COMMAND ----------

df2 = spark.read.csv("abfss://team3datalakeroot@team3openhack.dfs.core.windows.net/SQLInputSales/Customers.txt", header = True)

# COMMAND ----------

from pyspark.sql.functions import arrays_zip, col

(df
    .withColumn("tmp", arrays_zip("b", "c"))
    .withColumn("tmp", explode("tmp"))
    .select("a", col("tmp.b"), col("tmp.c"), "d"))
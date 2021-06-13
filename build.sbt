scalafmtOnCompile in Compile := true
name := "spark-tar"

version := "0.1"

crossScalaVersions := Seq("2.12.12")
scalaVersion := "2.12.12"
val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile"
)

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled", "-Duser.timezone=GMT")

updateOptions := updateOptions.value.withLatestSnapshots(false)

publishMavenStyle := true

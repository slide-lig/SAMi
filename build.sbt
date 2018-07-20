name := "gspanx"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-feature")
scalacOptions ++= Seq("-deprecation")
scalacOptions ++= Seq("-language:existentials")

libraryDependencies += "net.openhft" % "koloboke-api-jdk8" % "0.6.8"
libraryDependencies += "net.openhft" % "koloboke-impl-jdk8" % "0.6.8"
libraryDependencies += "org.eclipse.collections" % "eclipse-collections-api" % "9.1.0"
libraryDependencies += "org.eclipse.collections" % "eclipse-collections" % "9.1.0"
libraryDependencies += "commons-cli" % "commons-cli" % "1.4"
libraryDependencies += "com.google.code.findbugs" % "jsr305" % "3.0.2" % "compile"
libraryDependencies += "com.google.guava" % "guava" % "25.0-jre"

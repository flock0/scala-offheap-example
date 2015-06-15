scalaVersion := "2.11.6"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "sh.den" % "scala-offheap_2.11" % "0.1-SNAPSHOT"
libraryDependencies += "ch.epfl.data" % "sc-pardis-library_2.11" % "0.1-SNAPSHOT"
libraryDependencies += "lego-core" % "lego-core_2.11" % "0.1-SNAPSHOT"
libraryDependencies += "dblab-offheap" % "dblab-offheap_2.11" % "0.1-SNAPSHOT"
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)

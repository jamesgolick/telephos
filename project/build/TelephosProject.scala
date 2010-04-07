import sbt._
import Process._

class TelephosProject(info: ProjectInfo) extends DefaultProject(info) {
  val specs        = "org.specs" % "specs" % "1.6.0" from "http://specs.googlecode.com/files/specs-1.6.0.jar"
  val mongodb      = "com.mongodb" % "mongodb" % "1.0" from "http://cloud.github.com/downloads/mongodb/mongo-java-driver/mongo-1.0.jar"
  val mockito      = "org.mockito" % "mockito" % "1.8.0" from "http://mockito.googlecode.com/files/mockito-all-1.8.0.jar"

  def javaDirectoryPath           = "src_managed" / "main" / "java"
  override def mainJavaSourcePath = javaDirectoryPath
  def thriftDirectoryPath         = "src_managed" / "main" / "thrift"
  def thriftFile                  = thriftDirectoryPath / "cassandra.thrift"

  def thriftTask(tpe: String, directory: Path, thriftFile: Path) = {
    val cleanIt = cleanTask(directory) named("clean-thrift-" + tpe)
    val mkdir   = execTask { <x>mkdir {directory.absolutePath}</x> } named("mk-thrift-dir") dependsOn(cleanIt)
    execTask {
      <x>thrift --gen {tpe} -o {directory.absolutePath} {thriftFile.absolutePath}</x>
    } dependsOn(mkdir)
  }

  lazy val thriftJava = thriftTask("java", javaDirectoryPath, thriftFile) describedAs("Build Thift Java")

  override def compileAction = super.compileAction dependsOn(thriftJava)

  lazy val benchmark = task { args =>
    runTask(Some("com.protose.telephos.benchmarks"), runClasspath, args) dependsOn(compile, copyResources)
  } describedAs("Run the benchmarks.")

  val distName = "telephos.zip"

  def distPath = (
    // NOTE the double hashes (##) hoist the files in the preceeding directory
    // to the top level - putting them in the "base directory" in sbt's terminology
    ((outputPath ##) / defaultJarName) +++
    mainResources +++
    mainDependencies.scalaJars +++
    descendents(info.projectPath, "*.sh") +++
    descendents(info.projectPath, "*.rb") +++
    descendents(info.projectPath, "*.conf") +++
    descendents(info.projectPath / "lib" ##, "*.jar") +++
    descendents(managedDependencyRootPath / "compile" ##, "*.jar")
  )

  override def manifestClassPath = Some(
    distPath.getFiles
    .filter(_.getName.endsWith(".jar"))
    .map(_.getName).mkString(" ")
  )

  override def mainClass = Some("com.protose.telephos.benchmarks")
  override lazy val `package` = packageAction

  lazy val zip = zipTask(distPath, "dist", distName) dependsOn (`package`) describedAs("Zips up the project.")
}

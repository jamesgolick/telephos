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
}

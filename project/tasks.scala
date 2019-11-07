package ai.datahunters.sbtrelease
import sbt._
import Keys._

object ReleaseTasks {


  lazy val distTargetDirName = "dist"
  lazy val distLibDirName = "libs"
  lazy val configsDirName = "configs"
  lazy val scriptsDirName = "scripts"

  
  lazy val awsLibsDirName = "aws_libs"
  lazy val standaloneDirName = "standalone"
  lazy val distributedDirName = "distributed"

  lazy val distStandalone = taskKey[Unit]("Create release package for Standalone Mode")
  lazy val DistStandaloneConfig = config("Dist config for Standalone Mode")
  
  lazy val distDistributed = taskKey[Unit]("Create release package for Distributed Mode")
  lazy val AWSDepsConfig = config("Dist config for AWS dependencies")
  
  lazy val dist = taskKey[Unit]("Create distributable package")


  def build[T](assemblyTask: sbt.Def.Initialize[sbt.Task[T]]) = Seq(
    buildStandalone(),
    buildDistributed(),
    buildBundle(assemblyTask)
  )

  /**
    * Bundle Task creating whole distributable package including Distributed and Standalone Mode
    * @param assemblyTask External task responsible for creating fat JAR
    * @tparam T
    * @return
    */
  private def buildBundle[T](assemblyTask: sbt.Def.Initialize[sbt.Task[T]]) = {
    dist := Def.sequential(assemblyTask, distStandalone, distDistributed).value
  }

  /**
    * Task creating distributable package for Standalone mode
    * @return
    */
  private def buildStandalone() = {
    distStandalone := {
      (update in AWSDepsConfig).value.allFiles.foreach { f =>
        IO.copyFile(f, target.value / distTargetDirName / standaloneDirName / awsLibsDirName /  f.getName)
      }
      (update in DistStandaloneConfig).value.allFiles.foreach { f =>
        IO.copyFile(f, target.value / distTargetDirName / standaloneDirName / distLibDirName /  f.getName)
      }
      // Copying sample configs
      IO.copyDirectory(
        (Compile / resourceDirectory).value / configsDirName,
        target.value / distTargetDirName / standaloneDirName / configsDirName
      )
      // Copying running script
      IO.copyDirectory(
        (Compile / resourceDirectory).value / scriptsDirName / standaloneDirName,
        target.value / distTargetDirName / standaloneDirName
      )
      // Copying main JAR
      IO.copyFile(
        target.value / s"${name.value}-${version.value}.jar",
        target.value / distTargetDirName / standaloneDirName / s"${name.value}-${version.value}.jar"
      )
    }
  }

  /**
    * Task creating distributable package for Distribuded mode
    * @return
    */
  private def buildDistributed() = {
    distDistributed := {
      
      (update in AWSDepsConfig).value.allFiles.foreach { f =>
        IO.copyFile(f, target.value / distTargetDirName / distributedDirName / awsLibsDirName /  f.getName)
      }
      // Copying sample configs
      IO.copyDirectory(
        (Compile / resourceDirectory).value / configsDirName,
        target.value / distTargetDirName / distributedDirName / configsDirName
      )
      // Copying running script
      IO.copyDirectory(
        (Compile / resourceDirectory).value / scriptsDirName / distributedDirName,
        target.value / distTargetDirName / distributedDirName
      )
      // Copying main JAR
      IO.copyFile(
        target.value / s"${name.value}-${version.value}.jar",
        target.value / distTargetDirName / distributedDirName / s"${name.value}-${version.value}.jar"
      )

    }
  }
}

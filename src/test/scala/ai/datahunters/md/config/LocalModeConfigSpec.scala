package ai.datahunters.md.config

import ai.datahunters.md.UnitSpec
import com.typesafe.config.ConfigException

class LocalModeConfigSpec extends UnitSpec {

//  "A LocalModeConfig" should "build object from configuration file with all defaults" in {
//    val availableCores = Math.max(1, Runtime.getRuntime.availableProcessors - 1)
//    val config = LocalModeConfig.buildFromProperties(configPath("local.config.properties"))
//    assert(config.allowedDirectories === None)
//    assert(config.format === "json")
//    assert(config.outputDirPath === "sample_output")
//    assert(config.partitionsNum === availableCores * 2)
//    assert(config.inputPaths === Seq("/some/input/path"))
//    assert(config.maxMemoryGB === 2)
//    assert(config.allowedTags === None)
//    assert(config.cores === availableCores)
//    assert(config.outputFilesNum === 1)
//  }
//
//  it should "build object from configuration file without defaults" in {
//    val config = LocalModeConfig.buildFromProperties(configPath("full_local.config.properties"))
//    assert(config.allowedDirectories.get === Seq("JPEG", "MP4"))
//    assert(config.format === "json")
//    assert(config.outputDirPath === "sample_output")
//    assert(config.partitionsNum === 400)
//    assert(config.inputPaths === Seq("/some/input/path"))
//    assert(config.maxMemoryGB === 2)
//    assert(config.allowedTags.get === Seq("NumberOfDetectedFaces", "WorldTimeLocation"))
//    assert(config.cores === 20)
//    assert(config.outputFilesNum === 181)
//  }
//
//  it should "throw exception when there is no requierd configuration" in {
//    assertThrows[ConfigException] {
//      LocalModeConfig.buildFromProperties(configPath("empty.config.properties"))
//    }
//  }
}

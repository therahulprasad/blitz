package main

import (
	"os"
	"encoding/json"
	"net"
	"fmt"
	"log"
)

// load json configuration file into memory
func loadConfig(forceDebugMode bool) Configuration {
	file, _ := os.Open("config.json")
	decoder := json.NewDecoder(file)
	config  := Configuration{}
	err 	:= decoder.Decode(&config)
	failOnError(err, "Failed to load config file")

	if config.Rabbit.ReconnectWaitTimeSec < 1 {
		config.Rabbit.ReconnectWaitTimeSec = 1
	}

	if forceDebugMode {
		config.DebugMode = true
	}

	return config
}
func checkAndCreateDirectory(dirpath string) {
	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		err := os.MkdirAll(dirpath, os.FileMode(int(0777)))
		if err != nil {
			failOnError(err, "Directory creation error, while creating : " + dirpath)
		}
	}
}
func checkSystem(config Configuration) {
	// Check if sendmail exists
	if(config.SendMailPath != "") {
		if _, err := os.Stat(config.SendMailPath); os.IsNotExist(err) {
			log.Fatalf("Incorrect sendmail path in config: %s", err)
			panic(fmt.Sprintf("Incorrect sendmail path in config: %s", err))
		}
	}

	port := config.SingularityPort
	ln, err := net.Listen("tcp", ":" + port)
	if err != nil {
		// Exit if port is already occupied
		failOnError(err, "Another process is already using port " + port + " - " + err.Error())
	}
	go ln.Accept()


	// Check if log directory exists, if not create it, In case of error fail
	checkAndCreateDirectory(config.Logging.GcmErr.RootPath)
	if config.Logging.GcmErr.LogSuccess == true {
		checkAndCreateDirectory(config.Logging.GcmErr.SuccessPath)
	}

	checkAndCreateDirectory(config.Logging.ApnErr.RootPath)
	if config.Logging.ApnErr.LogSuccess == true {
		checkAndCreateDirectory(config.Logging.ApnErr.SuccessPath)
	}
	checkAndCreateDirectory(config.Logging.DbErr.RootPath)

	// TODO: Check if app error file can be created, if not exit
	// Check if PEM files needed for APN exists
	for i:=0;i<len(config.ApnQueues);i++ {
		_, err := os.Stat(config.ApnQueues[i].PemPath)
		if err != nil {
			failOnError(err, "Specified PEM file does not exists : " + config.ApnQueues[i].PemPath)
		}
	}

	// TODO: Names of all the queues must be different

	if len(config.GcmQueues) < 1 {
		log.Fatalf("Config: Queues not found")
		panic(fmt.Sprintf("Config: Queues not found"))
	}
}


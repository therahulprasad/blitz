package main

import (
	"os"
	"encoding/json"
	"flag"
	"net"
	"fmt"
	"log"
)

// load json configuration file into memory
func loadConfig() Configuration {
	file, _ := os.Open("config.json")
	decoder := json.NewDecoder(file)
	config  := Configuration{}
	err 	:= decoder.Decode(&config)
	failOnError(err, "Failed to load config file")

	if config.Rabbit.ReconnectWaitTimeSec < 1 {
		config.Rabbit.ReconnectWaitTimeSec = 1
	}

	debugModePtr := flag.Bool("debugmode", false, "true/false")
	if *debugModePtr {
		config.DebugMode = true
	}

	return config
}

func checkSystem(config Configuration) {
	port := config.SingularityPort
	ln, err := net.Listen("tcp", ":" + port)
	if err != nil {
		// Exit if port is already occupied
		failOnError(err, "Another process is already using port " + port + " - " + err.Error())
	}
	go ln.Accept()


	// Check if log directory exists, if not create it, In case of error fail
	if _, err := os.Stat(config.Logging.GcmErr.RootPath); os.IsNotExist(err) {
		err := os.MkdirAll(config.Logging.GcmErr.RootPath, os.FileMode(int(0777)))
		if err != nil {
			failOnError(err, "Directory creation error, while creating : " + config.Logging.GcmErr.RootPath)
		}
	}

	if _, err := os.Stat(config.Logging.DbErr.RootPath); os.IsNotExist(err) {
		err := os.MkdirAll(config.Logging.DbErr.RootPath, os.FileMode(int(0777)))
		if err != nil {
			failOnError(err, "Directory creation error, while creating : " + config.Logging.DbErr.RootPath)
		}
	}

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


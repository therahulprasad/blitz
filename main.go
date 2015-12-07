package main

/**
	TODO: Run as a service
	TODO: Do not panic and die
	TODO: Inform Admin worker to send mail for first case of 10 continuous GCM error, +15 min same error count (more than half of expected)

	DONE: Peaceful quit, wait for all worker to finish before quitting
	DONE: What happens if RabbitMQ restarts ? >> Logic for reconnect
	DONE: On continuous 10 GCM error, everyworker should hold for 1 minute before trying again
	TODO: Multiple trial before discarding and check before requeing
	TODO: % encode vhost name
	TODO: To create Queues or not should be configurable as user might not have permission to create queue
	TODO: Write test cases
	TODO: Setup travis
	DONE: Add timestamp to logs
	TODO: handle log separetly, Dont process it one by one
	DONE: Add worker information to logs
	TODO: Separate logs for separate workers
	TODO: App error should be kept in proper way
**/

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"encoding/json"
	"os"
	"strconv"
	"flag"
	"time"
	"os/signal"
	"syscall"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

// to load configuration file into memory
func loadConfig() Configuration {
	file, _ := os.Open("config.json")
	decoder := json.NewDecoder(file)
	config  := Configuration{}
	err 	:= decoder.Decode(&config)
	failOnError(err, "Failed to load config file")

	if config.NumWorkers <= 0 {
		config.NumWorkers = 1
	}
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
	// Check if log directory exists, if not create it, In case of error fail
	if _, err := os.Stat(config.Logging.GcmErr.RootPath); os.IsNotExist(err) {
		err := os.MkdirAll(config.Logging.GcmErr.RootPath, os.FileMode(int(0777)))
		if err != nil {
			failOnError(err, "Directory creation error : " + err.Error())
		}
	}
}

func initConn(config Configuration) *amqp.Connection {
	olog("Connecting", config.DebugMode)

	conn, err := amqp.Dial("amqp://" + config.Rabbit.Username + ":" + config.Rabbit.Password + "@" + config.Rabbit.Host + ":" + strconv.Itoa(config.Rabbit.Port) + "/" + config.Rabbit.Vhost)
	if err != nil {
		ticker := time.NewTicker(time.Second * time.Duration(config.Rabbit.ReconnectWaitTimeSec))
		for range ticker.C {
			olog(fmt.Sprintf("Err: %s, Trying to reconnect", err.Error()), config.DebugMode)
			conn, err = amqp.Dial("amqp://" + config.Rabbit.Username + ":" + config.Rabbit.Password + "@" + config.Rabbit.Host + ":" + strconv.Itoa(config.Rabbit.Port) + "/" + config.Rabbit.Vhost)
			// TODO: Log error in file
			if err == nil {
				ticker.Stop()
				break
			}
		}
	}
	return conn
}

func main() {
	// Load configuration
	config 	:= loadConfig()
	// Check if basis requirements are fullfilled
	checkSystem(config)

	killWorker := make(chan int)

	ae, err := os.OpenFile(config.Logging.AppErr.FilePath, os.O_CREATE | os.O_APPEND | os.O_WRONLY, 0666)
	failOnError(err, "Unable to open App Error Log file")
	logger := log.New(ae, "", log.LstdFlags | log.Lshortfile)
	logger.Printf("Starting Service. BTW, its not an error ;)")

	ch_gcm_err := make(chan []byte, config.NumWorkers * 2) // Create a buffered channel so that processor won't block witing for other to write into error log
	go logErrToFile(config.Logging.GcmErr.RootPath, ch_gcm_err, config.DebugMode)

	// Create channel for killing inactive goroutines
	killStatusInactive := make(chan int)
	killTokenUpd := make(chan int)
	killTokenUpdAck := make(chan int)
	killStatusInactiveAck := make(chan int)

	// Init Connection with RabbitMQ
//	conn, err := amqp.Dial("amqp://" + config.Rabbit.Username + ":" + config.Rabbit.Password + "@" + config.Rabbit.Host + ":" + strconv.Itoa(config.Rabbit.Port) + "/")
//	failOnError(err, "Failed to connect to RabbitMQ")
	conn := initConn(config)
	defer conn.Close()

	// Create channel for reading messages
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	if config.Rabbit.CreateQueues {
		_, err = ch.QueueDeclare(
			config.Rabbit.GcmMsgQueue, // name
			true,         // durable
			false,        // delete when unused
			false,        // exclusive
			false,        // no-wait
			nil,          // arguments
		)
		failOnError(err, "Failed to declare a queue")

		err = ch.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		failOnError(err, "Failed to set QoS")

		_, err = ch.QueueDeclare(
			config.Rabbit.GcmTokenUpdateQueue, // name
			true,         // durable
			false,        // delete when unused
			false,        // exclusive
			false,        // no-wait
			nil,          // arguments
		)
		failOnError(err, "Failed to declare a queue")

		_, err = ch.QueueDeclare(
			config.Rabbit.GcmStatusInactiveQueue, // name
			true,         // durable
			false,        // delete when unused
			false,        // exclusive
			false,        // no-wait
			nil,          // arguments
		)
		failOnError(err, "Failed to declare a queue")

	}

	chQuit := make(chan os.Signal, 2)
	signal.Notify(chQuit, os.Interrupt, syscall.SIGTERM)

	// Function to handle quit singal
	go func(chQuit chan os.Signal, config Configuration, killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck chan int) {
		// Wait for quit event
		<-chQuit

		olog(fmt.Sprintf("Killing all workers"), config.DebugMode)
		for i:=0; i<config.NumWorkers; i++ {
			killWorker<- 1
		}

		// Send signal to kill worker
		killStatusInactive<- NeedAck
		killTokenUpd<- NeedAck

		// Wait for both the goroutines to complete
		<-killStatusInactiveAck
		<-killTokenUpdAck

		// Exit peacefully
		os.Exit(1)
	}(chQuit, config, killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck)


	olog(fmt.Sprintf("Spinning up %d workers", config.NumWorkers), config.DebugMode)
	for i:=0; i<config.NumWorkers; i++ {
		go gcm_processor(i, config, conn, config.Rabbit.GcmTokenUpdateQueue, config.Rabbit.GcmStatusInactiveQueue, config.Rabbit.GcmMsgQueue, ch_gcm_err, logger, killWorker)
	}
	olog(fmt.Sprintf("Startting workers for tokenUpdate and status_inactive"), config.DebugMode)
	go gcm_error_processor_status_inactive(config, conn, config.Rabbit.GcmStatusInactiveQueue, ch_gcm_err, logger, killStatusInactive, killStatusInactiveAck)
	go gcm_error_processor_token_update(config, conn, config.Rabbit.GcmTokenUpdateQueue, ch_gcm_err, logger, killTokenUpd, killTokenUpdAck)


	// If connection is closed restart
	reset := conn.NotifyClose(make(chan *amqp.Error))
	for range reset {
		go restart(reset, config, conn, config.Rabbit.GcmStatusInactiveQueue, config.Rabbit.GcmTokenUpdateQueue, config.Rabbit.GcmMsgQueue, ch_gcm_err, logger, killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck)
	}

	olog(fmt.Sprintf("[*] Waiting for messages. To exit press CTRL+C"), config.DebugMode)
	forever := make(chan bool)
	<-forever
}

// Function to restart everything
func restart(reset chan *amqp.Error, config Configuration, conn *amqp.Connection, GcmStatusInactiveQueueName, GcmTokenUpdateQueueName, GcmQueueName string, ch_gcm_err chan []byte, logger *log.Logger,
			killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck chan int) {
	// Kill all Worker
	for i:=0; i<config.NumWorkers; i++ {
		killWorker<- 1
	}

	killStatusInactive<- NoAckNeeded
	killTokenUpd<- NoAckNeeded

	conn.Close()
	conn = initConn(config)
	defer conn.Close()

	olog(fmt.Sprintf("Spinning up %d workers", config.NumWorkers), config.DebugMode)
	for i:=0; i<config.NumWorkers; i++ {
		go gcm_processor(i, config, conn, GcmTokenUpdateQueueName, GcmStatusInactiveQueueName, GcmQueueName, ch_gcm_err, logger, killWorker)
	}

	olog("Starting error processors", config.DebugMode)
	go gcm_error_processor_status_inactive(config, conn, GcmStatusInactiveQueueName, ch_gcm_err, logger, killStatusInactive, killStatusInactiveAck)
	go gcm_error_processor_token_update(config, conn, GcmTokenUpdateQueueName, ch_gcm_err, logger, killTokenUpd, killTokenUpdAck)

	reset = conn.NotifyClose(make(chan *amqp.Error))
	for range reset {
		go restart(reset, config, conn, GcmStatusInactiveQueueName, GcmTokenUpdateQueueName, GcmQueueName, ch_gcm_err, logger, killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck)
	}
}

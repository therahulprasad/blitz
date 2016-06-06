package main

/**
	TODO: Run as a service script in version control
	TODO: sh file to install it as service
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
	TODO: App error should be kept in proper way
	DONE: Do not start this app, if its already running (pgrep blitz) Done using listening to port
	TODO: Implement multiple types of GCM Error (Not Registed / Invalid....)
	DONE: After every database call, goroutine should wait for few seconds, configurable
	TODO: Sucess log for database writes should be configurable
	DONE: TransactionMinCount from config not working
	DONE: Add support for multiple queues with separate API keys
	TODO: BUG: Can not Quit application while application is trying to reconnect to rabbitmq
	TODO: Combine olog and logger.Printf into single function
	DONE: Implement separate log for GCM and Databaseb
	DONE: Prevent infinite requeing
	DONE: Requeue count from config
	TODO: Inform admin if global application based send failure count increaases specified value
	TODO: Logs file permission / update user
	DONE: Separate folder for error and success / GCM and APN
		-- Create channel for APN error
		-- Add APN log file to config
		-- Make sure folder exists
**/

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"strconv"
	"time"
	"os/signal"
	"syscall"
	"github.com/streamrail/concurrent-map"
)

/**
 * In case of error it displays error and exits
 * @param err Error object
 * @param msg String message to be displayed
 */
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("FailOnError %s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}


/**
* initConn() Recursivly tries to create connection with RabbitMQ server.
* @param config Hello
*/
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

var retries_gcm cmap.ConcurrentMap
var retries_apn cmap.ConcurrentMap

func main() {
	// Load configuration
	config 	:= loadConfig()

	// Check if basis requirements are fullfilled
	checkSystem(config)

	// A channel which is used to kill all workers
	killWorker := make(chan int)

	// Open file for loffing file error
	ae, err := os.OpenFile(config.Logging.AppErr.FilePath, os.O_CREATE | os.O_APPEND | os.O_WRONLY, 0666)
	failOnError(err, "Unable to open App Error Log file")

	// Create a logger with opened file to log application errors
	logger := log.New(ae, "", log.LstdFlags | log.Lshortfile)
	logger.Printf("Starting Service...")

	retries_gcm = cmap.New()
	retries_apn = cmap.New()

	// TODO: Get number of worker buffer from config file
	// Create buffered channel for writing GCM log
	ch_gcm_log := make(chan []byte, 100) // Create a buffered channel so that processor won't block witing for other to write into error log
	ch_gcm_log_success := make(chan []byte, 100) // Create a buffered channel so that processor won't block witing for other to write into error log
	ch_apn_log := make(chan []byte, 100) // Create a buffered channel so that processor won't block witing for other to write into error log
	ch_apn_log_success := make(chan []byte, 100) // Create a buffered channel so that processor won't block witing for other to write into error log

	// Create buffered channel for writing db log
	ch_db_log := make(chan []byte, 100) // Create a buffered channel so that processor won't block witing for other to write into error log

	// Create goroutine for logging gcm error log. Its a separate goroutine to make it non blocking
	go logErrToFile(config.Logging.GcmErr.RootPath, ch_gcm_log, config.DebugMode)
	if config.Logging.GcmErr.LogSuccess == true {
		go logErrToFile(config.Logging.GcmErr.SuccessPath, ch_gcm_log_success, config.DebugMode)
	}

	// Create goroutine for logging apn error log. Its a separate goroutine to make it non blocking
	go logErrToFile(config.Logging.ApnErr.RootPath, ch_apn_log, config.DebugMode)
	if config.Logging.ApnErr.LogSuccess == true {
		go logErrToFile(config.Logging.ApnErr.SuccessPath, ch_apn_log_success, config.DebugMode)
	}

	// Create goroutine for logging db error log. Its a separate goroutine to make it non blocking
	go logErrToFile(config.Logging.DbErr.RootPath, ch_db_log, config.DebugMode)

	// channel for killing status-inactive goroutines
	killStatusInactive := make(chan int)
	killApnStatusInactive := make(chan int)

	// channel for killing token-update goroutines
	killTokenUpd := make(chan int)

	// channel for receiving ack that token-update goroutine is killed
	killTokenUpdAck := make(chan int)

	// channel for receiving ack that status-inactive goroutine is killed
	killStatusInactiveAck := make(chan int)
	killApnStatusInactiveAck := make(chan int)

	// Init Connection with RabbitMQ
	conn := initConn(config)
	defer conn.Close()

	// Create channel for reading messages
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Create Queues if configured to do so.
	createQueues(config, ch)

	chQuit := make(chan os.Signal, 2)
	signal.Notify(chQuit, os.Interrupt, syscall.SIGTERM)

	// Function to handle quit singal
	go func(chQuit chan os.Signal, config Configuration, killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck, killApnStatusInactive, killApnStatusInactiveAck chan int) {
		// Wait for quit event
		<-chQuit

		olog(fmt.Sprintf("Killing all workers"), config.DebugMode)
		// TODO: APN All logic to kill APN workers
		killAllWorkers(config, killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck, killApnStatusInactive, killApnStatusInactiveAck)

		// Exit peacefully
		os.Exit(1)
	}(chQuit, config, killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck, killApnStatusInactive, killApnStatusInactiveAck)


	olog(fmt.Sprintf("Spinning up workers"), config.DebugMode)
	// For all GcmQueues start new goroutines
	for i:=0; i < len(config.GcmQueues); i++ {
		// For all GCM Queues start workers
		for j:=0; j<config.GcmQueues[i].Numworkers; j++ {
			go gcm_processor(j, config, conn, config.GcmQueues[i].GcmTokenUpdateQueue, config.GcmQueues[i].GcmStatusInactiveQueue,
								config.GcmQueues[i].Name, ch_gcm_log, ch_gcm_log_success, logger, killWorker, config.GcmQueues[i])
		}

		olog(fmt.Sprintf("Startting workers for tokenUpdate and status_inactive for GCM %s", config.GcmQueues[i].Identifier), config.DebugMode)
		go gcm_error_processor_status_inactive(config, conn, config.GcmQueues[i].GcmStatusInactiveQueue, ch_db_log, logger, killStatusInactive, killStatusInactiveAck, config.GcmQueues[i])
		go gcm_error_processor_token_update(config, conn, config.GcmQueues[i].GcmTokenUpdateQueue, ch_db_log, logger, killTokenUpd, killTokenUpdAck, config.GcmQueues[i])
	}

	//For all APN Queues start workers
	for i:=0; i < len(config.ApnQueues); i++ {
		// For all GCM Queues start workers
		for j:=0; j<config.ApnQueues[i].NumWorkers; j++ {
			go apn_processor(j, config, conn, config.ApnQueues[i].ApnStatusInactiveQueue,
				config.ApnQueues[i].Name, ch_apn_log, ch_apn_log_success, logger, killWorker, config.ApnQueues[i].Topic, config.ApnQueues[i].PemPath, config.ApnQueues[i].IsHourly)
		}

		olog(fmt.Sprintf("Startting workers for status_inactive for APN %s", config.GcmQueues[i].Identifier), config.DebugMode)
		go apn_error_processor_status_inactive(config, conn, config.ApnQueues[i].ApnStatusInactiveQueue, ch_db_log, logger, killApnStatusInactive, killApnStatusInactiveAck, config.ApnQueues[i])
	}

	// If connection is closed restart
	reset := conn.NotifyClose(make(chan *amqp.Error))
	for range reset {
		go restart(reset, config, conn, ch_gcm_log, ch_db_log, ch_apn_log, ch_gcm_log_success, ch_apn_log_success, logger, killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck, killApnStatusInactive, killApnStatusInactiveAck)
	}

	// Run forever until excited using SIGTERM
	forever := make(chan bool)
	<-forever
}


/**
 * It sends kill signal through kill channel to all go routines
 */
func killAllWorkers(config Configuration, killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck, killApnStatusInactive, killApnStatusInactiveAck chan int) {
	// Kill All GCM workers
	for i:=0; i < len(config.GcmQueues); i++ {
		for j := 0; j < config.GcmQueues[i].Numworkers; j++ {
			killWorker<- 1
		}
		// Kill status inactive token processor worker for GCM
		killStatusInactive<- NeedAck

		// Kill update token processor worker for GCM
		killTokenUpd<- NeedAck
	}

	// Kill All APN workers
	for i:=0; i < len(config.ApnQueues); i++ {
		for j := 0; j < config.ApnQueues[i].NumWorkers; j++ {
			killWorker<- 1
		}

		// Kill status inactive token processor worker for GCM
		killApnStatusInactive<- NeedAck
	}

	// Wait for database goroutines to end
	olog("Waiting for GCM Status Inactive service to end", config.DebugMode)
	<-killStatusInactiveAck
	olog("Waiting for GCM Token Update service to end", config.DebugMode)
	<-killTokenUpdAck
	olog("Waiting for APN Status Inactive service to end", config.DebugMode)
	<-killApnStatusInactiveAck
}

// Function to restart everything
func restart(reset chan *amqp.Error, config Configuration, conn *amqp.Connection, ch_gcm_log, ch_db_log, ch_apn_log, ch_gcm_log_success, ch_apn_log_success chan []byte, logger *log.Logger,
			killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck, killApnStatusInactive, killApnStatusInactiveAck chan int) {
	// Kill all Worker
	killAllWorkers(config, killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck, killApnStatusInactive, killApnStatusInactiveAck)

	conn.Close()
	conn = initConn(config)
	defer conn.Close()

	olog(fmt.Sprintf("Spinning up workers"), config.DebugMode)
	// For all GcmQueues start new goroutines
	for i:=0; i < len(config.GcmQueues); i++ {
		for j:=0; j<config.GcmQueues[i].Numworkers; j++ {
			go gcm_processor(j, config, conn, config.GcmQueues[i].GcmTokenUpdateQueue, config.GcmQueues[i].GcmStatusInactiveQueue,
				config.GcmQueues[i].Name, ch_gcm_log, ch_gcm_log_success, logger, killWorker, config.GcmQueues[i])
		}
		go gcm_error_processor_status_inactive(config, conn, config.GcmQueues[i].GcmStatusInactiveQueue, ch_db_log, logger, killStatusInactive, killStatusInactiveAck, config.GcmQueues[i])
		go gcm_error_processor_token_update(config, conn, config.GcmQueues[i].GcmTokenUpdateQueue, ch_db_log, logger, killTokenUpd, killTokenUpdAck, config.GcmQueues[i])
	}
	//For all APN Queues start workers
	for i:=0; i < len(config.ApnQueues); i++ {
		// For all GCM Queues start workers
		for j:=0; j<config.ApnQueues[i].NumWorkers; j++ {
			go apn_processor(j, config, conn, config.ApnQueues[i].ApnStatusInactiveQueue,
				config.ApnQueues[i].Name, ch_apn_log, ch_apn_log_success, logger, killWorker, config.ApnQueues[i].Topic, config.ApnQueues[i].PemPath, config.ApnQueues[i].IsHourly)
		}

		olog(fmt.Sprintf("Startting workers for status_inactive for APN %s", config.GcmQueues[i].Identifier), config.DebugMode)
		go apn_error_processor_status_inactive(config, conn, config.ApnQueues[i].ApnStatusInactiveQueue, ch_db_log, logger, killApnStatusInactive, killApnStatusInactiveAck, config.ApnQueues[i])
	}


	olog("Starting error processors", config.DebugMode)


	reset = conn.NotifyClose(make(chan *amqp.Error))
	for range reset {
		go restart(reset, config, conn, ch_gcm_log, ch_db_log, ch_apn_log, ch_gcm_log_success, ch_apn_log_success, logger, killWorker, killStatusInactive, killTokenUpd, killStatusInactiveAck, killTokenUpdAck, killApnStatusInactive, killApnStatusInactiveAck)
	}
}

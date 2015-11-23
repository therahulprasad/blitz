package main

/**
	TODO: Run as a service
	TODO: Do not panic and die
	TODO: Inform Admin worker to send mail for first case of 10 continuous GCM error, +15 min same error count (more than half of expected)

	DONE: Peaceful quit, wait for all worker to finish before quitting
	DONE: What happens if RabbitMQ restarts ? >> Logic for reconnect
	DONE: On continuous 10 GCM error, everyworker should hold for 1 minute before trying again
	TODO: Multiple trial before discarding and check before requeing
**/

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"encoding/json"
	"os"
	"strconv"
	"github.com/alexjlockwood/gcm"
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

	if(config.NumWorkers <= 0) {
		config.NumWorkers = 1
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
		ticker := time.NewTicker(time.Second * 5) // TODO: Sould be Configurable
		for range ticker.C {
			olog(fmt.Sprintf("Err: %s, Trying to reconnect", err.Error()), config.DebugMode)
			conn, err = amqp.Dial("amqp://" + config.Rabbit.Username + ":" + config.Rabbit.Password + "@" + config.Rabbit.Host + ":" + strconv.Itoa(config.Rabbit.Port) + "/")
			// TODO: Log error in file
			if err == nil {
				ticker.Stop()
				break
			}
		}
	}
	//	failOnError(err, "Failed to connect to RabbitMQ")
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

	ch_gcm_err := make(chan []byte, config.NumWorkers * 2) // Create a buffered channel so that processor won't block witing for other to write into error log
	go logErrToFile(config.Logging.GcmErr.RootPath, ch_gcm_err, config.DebugMode)

	// Init Connection with RabbitMQ
//	conn, err := amqp.Dial("amqp://" + config.Rabbit.Username + ":" + config.Rabbit.Password + "@" + config.Rabbit.Host + ":" + strconv.Itoa(config.Rabbit.Port) + "/")
//	failOnError(err, "Failed to connect to RabbitMQ")
	conn := initConn(config)
	defer conn.Close()

	// Create channel for reading messages
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	gcmQueue, err := ch.QueueDeclare(
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

	GcmTokenUpdateQueue, err := ch.QueueDeclare(
		config.Rabbit.GcmTokenUpdateQueue, // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	GcmStatusInactiveQueue, err := ch.QueueDeclare(
		config.Rabbit.GcmStatusInactiveQueue, // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	olog(fmt.Sprintf("Spinning up %d workers", config.NumWorkers), config.DebugMode)
	for i:=0; i<config.NumWorkers; i++ {
		go gcm_processor(i, config, conn, GcmTokenUpdateQueue.Name, GcmStatusInactiveQueue.Name, gcmQueue.Name, ch_gcm_err, logger, killWorker)
	}

	chQuit := make(chan os.Signal, 2)
	signal.Notify(chQuit, os.Interrupt, syscall.SIGTERM)

	// Function to handle quit singal
	go func(chQuit chan os.Signal, config Configuration, killWorker chan int) {
		// Wait for quit event
		<-chQuit

		olog(fmt.Sprintf("Killing all workers"), config.DebugMode)
		for i:=0; i<config.NumWorkers; i++ {
			killWorker<- 1
		}

		// Exit peacefully
		os.Exit(1)
	}(chQuit, config, killWorker)

	// If connection is closed restart
	reset := conn.NotifyClose(make(chan *amqp.Error))
	for range reset {
		go restart(reset, config, conn, GcmStatusInactiveQueue.Name, GcmTokenUpdateQueue.Name, gcmQueue.Name, ch_gcm_err, logger, killWorker)
	}

	olog(fmt.Sprintf("[*] Waiting for messages. To exit press CTRL+C"), config.DebugMode)
	forever := make(chan bool)
	<-forever
}

// Function to restart everything
func restart(reset chan *amqp.Error, config Configuration, conn *amqp.Connection, GcmStatusInactiveQueueName, GcmTokenUpdateQueueName, GcmQueueName string, ch_gcm_err chan []byte, logger *log.Logger, killWorker chan int) {
	// Kill all Worker
	for i:=0; i<config.NumWorkers; i++ {
		killWorker<- 1
	}

	conn.Close()
	conn = initConn(config)
	defer conn.Close()

	olog(fmt.Sprintf("Spinning up %d workers", config.NumWorkers), config.DebugMode)
	for i:=0; i<config.NumWorkers; i++ {
		go gcm_processor(i, config, conn, GcmTokenUpdateQueueName, GcmStatusInactiveQueueName, GcmQueueName, ch_gcm_err, logger, killWorker)
	}

	reset = conn.NotifyClose(make(chan *amqp.Error))
	for range reset {
		go restart(reset, config, conn, GcmStatusInactiveQueueName, GcmTokenUpdateQueueName, GcmQueueName, ch_gcm_err, logger, killWorker)
	}
}


func gcm_processor(identity int, config Configuration, conn *amqp.Connection, GcmTokenUpdateQueueName, GcmStatusInactiveQueueName, GcmQueueName string, ch_gcm_err chan []byte, logger *log.Logger, killWorker chan int) {
	sender := &gcm.Sender{ApiKey: config.GCM.ApiKey}

	// Create new channel for Token update
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgsGcm, err := ch.Consume(
		GcmQueueName, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	// Get a message
	// Process it
	// If processing is complete, then delete it

	gcmErrCount := 0
	for {
		select {
		case <-killWorker:
			olog(fmt.Sprintf("%d Received kill command", identity), config.DebugMode)
			return
		case d := <-msgsGcm:
			olog(fmt.Sprintf("%d Received a message: %s", identity, d.Body), config.DebugMode)

			// GCM work
			payload  := Message{}
			err := json.Unmarshal(d.Body, &payload)
			//		failOnError(err, "Unable to parse data from message queue")
			if err != nil {
				logger.Printf("Unmarshal error = %s, for MQ message data = %s", err.Error(), d.Body)
				olog(fmt.Sprintf("Unmarshal error = %s, for MQ message data = %s", err.Error(), d.Body), config.DebugMode)
			} else {
				data 	:= payload.Body
				regIDs	:= payload.Token

				msg := gcm.NewMessage(data, regIDs...)
				response, err := sender.Send(msg, 2)
				if err != nil {
					gcmErrCount++
					logger.Printf("GCM send error = %s, data=%s",err.Error())
					olog(fmt.Sprintf("GCM send error = %s",err.Error()), config.DebugMode)

					// In case of GCM error / Requeue and continue to next
					// TODO: Inform admin

					// Same error continues without break for 10 times, sleep for a minute
					if gcmErrCount >= 10 {
						time.Sleep(time.Minute)
					}

					d.Nack(false, true)
					continue
				} else {
					gcmErrCount = 0
					for i, result := range response.Results {
						gcmError, _ := json.Marshal(GcmError{Result:result, OldToken:payload.Token[i], MulticastId:response.MulticastID})
						if result.Error == "NotRegistered" || result.Error == "InvalidRegistration" {
							statusInactiveMsg := GcmStatusInactiveMsg{Token:payload.Token[i]}
							jsonStatusInactiveMsg, err := json.Marshal(statusInactiveMsg)
							if err != nil {
								logger.Printf("GCM status inactive Marshal error = %s",err.Error())
								olog(fmt.Sprintf("GCM status inactive Marshal error = %s",err.Error()), config.DebugMode)
							}

							err = ch.Publish(
								"",           // exchange
								GcmStatusInactiveQueueName,       // routing key
								false,        // mandatory
								false,
								amqp.Publishing{
									DeliveryMode: amqp.Persistent,
									ContentType:  "text/json",
									Body:         jsonStatusInactiveMsg,
								})
							if err != nil {
								logger.Printf("GCM status inactive Publish error = %s",err.Error())
								olog(fmt.Sprintf("GCM status inactive Publish error = %s",err.Error()), config.DebugMode)
							}
							ch_gcm_err <- gcmError
						} else if result.RegistrationID != "" && result.MessageID != "" {
							// Send to Queue -> gcm_token_update
							tokenUpdateMsg := GcmTokenUpdateMsg{OldToken:payload.Token[i], NewToken:result.RegistrationID}
							jsonTokenUpdateMsg, err := json.Marshal(tokenUpdateMsg)
							if err != nil {
								logger.Printf("GCM RegistrationID update error = %s",err.Error())
								olog(fmt.Sprintf("GCM RegistrationID update error = %s",err.Error()), config.DebugMode)
							}

							err = ch.Publish(
								"",           // exchange
								GcmTokenUpdateQueueName,       // routing key
								false,        // mandatory
								false,
								amqp.Publishing{
									DeliveryMode: amqp.Persistent,
									ContentType:  "text/json",
									Body:         jsonTokenUpdateMsg,
								})
							if err != nil {
								logger.Printf("GCM RegistrationID update error = %s",err.Error())
								olog(fmt.Sprintf("GCM RegistrationID update error = %s",err.Error()), config.DebugMode)
							}
						} else if result.Error == "DeviceMessageRateExceeded" {
							// The rate of messages to a particular device is too high. Reduce the number of messages sent to
							// this device and do not immediately retry sending to this device.
							// Todo: Maybe send negative acknowledgement or move to another queue
							ch_gcm_err <- gcmError
						} else if result.Error == "TopicsMessageRateExceeded" {
							// The rate of messages to subscribers to a particular topic is too high. Reduce the number of messages
							// sent for this topic, and do not immediately retry sending.
							// Todo: Maybe send negative acknowledgement or move to another queue
							ch_gcm_err <- gcmError
						} else {
							ch_gcm_err <- gcmError
						}
					}
				}
			}
			// Acknowledge to MQ that work has been processed successfully
			d.Ack(false)
		}
	}
}

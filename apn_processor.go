package main

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
	"github.com/sideshow/apns2/certificate"
	apns "github.com/sideshow/apns2"
)

/**
 * An APN Worker which reads from RabbitMQ queue and sends APN messages
 * If APN is expired or invalid it sends APN key to a queue which is processed by another goroutine and updated in database
 */
func apn_processor(identity int, config Configuration, conn *amqp.Connection,
	ApnStatusInactiveQueueName, ApnQueueName string, ch_gcm_err chan []byte, logger *log.Logger,
	killWorker chan int, apnTopic string) {

	// Load PEM file specified in config file required to send APN messages
	cert, pemErr := certificate.FromPemFile("/Users/rahulprasad/GD/PEMs/ios_certificate.pem", "")
	failOnError(pemErr, "Failed to load PEM file for APN")

	// Create a new APN Client
	client := apns.NewClient(cert).Production()

	// Create new channel to communicate with RabbitMQ
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
		ApnQueueName, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	failOnError(err, "Failed to register a consumer")

	// Get a message
	// Process it
	// If processing is complete, then delete it
	for {
		select {
		case <-killWorker:
		// If a message is receieved through killWorker then log and exit
		olog(fmt.Sprintf("%d Received kill command", identity), config.DebugMode)
		return

		case d, ok := <-msgsGcm:
		//  Receieve messages from RabbitMQ,
		if !ok {
			// If channel is closed then wait for it to be reconnected or wait for kill signal
			time.Sleep(100 * time.Millisecond)
			continue
		}
		olog(fmt.Sprintf("%d Worker Received a message: %s", identity, d.Body), config.DebugMode)

		// Receive APN message from channel and decode it
		payload := ApnMessage{}
		err := json.Unmarshal(d.Body, &payload)

		// In case message coult not be decoded log error and skip
		if err != nil {
			logger.Printf("Unmarshal error = %s, for MQ message data = %s", err.Error(), d.Body)
			olog(fmt.Sprintf("Unmarshal error = %s, for MQ message data = %s", err.Error(), d.Body), config.DebugMode)

			// Acknowledge to MQ that work has been processed successfully
			d.Ack(false)
			continue
		}

		// Extract Body and Token from Payload
		data := payload.Body
		token := payload.Token

		// Create new APN notification
		notification := &apns.Notification{}

		// Add device token
		notification.DeviceToken = token
		notification.Topic = apnTopic
		notification.Payload, err = json.Marshal(data)

		// Could not encode map to json.
		if err != nil {
			logger.Printf("Marshal error = %s, for MQ message data = %s", err.Error(), d.Body)
			olog(fmt.Sprintf("Marshal error = %s, for MQ message data = %s", err.Error(), d.Body), config.DebugMode)
			d.Ack(false)
			continue
		}

		res, err := client.Push(notification)
		fmt.Println(token)
		fmt.Println(res)
		if err != nil {
			// gcmErrCount++
			logger.Printf("APN send error = %s, data=%s", err.Error())
			olog(fmt.Sprintf("APN send error = %s", err.Error()), config.DebugMode)

			// In case of APN error / Requeue and continue to next
			// TODO: Inform admin

			// Same error continues without break for 10 times, sleep for a minute
			// TODO: Implement this in a better way
			//					if gcmErrCount >= 10 {
			//						time.Sleep(time.Minute)
			//					}

			d.Nack(false, true)
			continue
		} else {
			// gcmErrCount = 0
			// TODO: Implement this as proper goroutine, its dangereous if user exists before this goroutine ends
			// Running result processor as a goroutine so that current worker can proceed with sending another APN request to
			// server, without getting delayed by processing
			go func() {
				isSentToClientSuccesfully := StatusErrGcmError
				t := time.Now()
				ts := t.Format(time.RFC3339)

				if res.StatusCode == 200 {
					// TODO: Process Success
					// Success
					isSentToClientSuccesfully = StatusSuccessApnRequest
				} else if(res.Reason == "BadDeviceToken" || res.Reason == "Unregistered") {
					// Token error, set status of this token as inactive in database
					// Create a Status Inactive Message to be sent to RabbitMQ queue
					statusInactiveMsg := ApnStatusInactiveMsg{Token: token}

					// Convert it into json byte array
					jsonStatusInactiveMsg, err := json.Marshal(statusInactiveMsg)

					// If there is an issue while conversion, log it and skip
					if err != nil {
						logger.Printf("APN status inactive Marshal error = %s", err.Error())
						olog(fmt.Sprintf("APN status inactive Marshal error = %s", err.Error()), config.DebugMode)
					} else {
						// Publish message to RabbitMQ
						err = ch.Publish(
							"", // exchange
							ApnStatusInactiveQueueName, // routing key
							false, // mandatory
							false,
							amqp.Publishing{
								DeliveryMode: amqp.Persistent,
								ContentType:  "text/json",
								Body:         jsonStatusInactiveMsg,
							})

						// If there is error while publishing message log it
						if err != nil {
							logger.Printf("APN status inactive Publish error = %s", err.Error())
							olog(fmt.Sprintf("APN status inactive Publish error = %s", err.Error()), config.DebugMode)
						}
					}
				} else if (res.Reason == "TooManyRequests") {
					// TODO: Delay sending instead of skipping
					// Skip sending to this token
					isSentToClientSuccesfully = StatusErrApnError
					return
				}

				apnLog, err := json.Marshal(ApnLog{TimeStamp: ts, Type: isSentToClientSuccesfully, ApnId: token, Data: ApnError{Reason:res.Reason}})
				if err != nil {
					logger.Printf("Marshal error while logging APN response = %s", err.Error())
					olog(fmt.Sprintf("Marshal error while logging APN response = %s", err.Error()), config.DebugMode)
				}
				ch_gcm_err <- apnLog
			}()
		}
		// Acknowledge to MQ that work has been processed successfully
		d.Ack(false)
		}
	}
}

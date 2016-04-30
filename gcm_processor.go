package main

import (
	"encoding/json"
	"fmt"
	"github.com/alexjlockwood/gcm"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func gcm_processor(identity int, config Configuration, conn *amqp.Connection, GcmTokenUpdateQueueName,
	GcmStatusInactiveQueueName, GcmQueueName string, ch_gcm_err chan []byte, logger *log.Logger,
	killWorker chan int, gcmQueue GcmQueue) {
	sender := &gcm.Sender{ApiKey: gcmQueue.ApiKey}

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

	//	gcmErrCount := 0
	for {
		select {
		case <-killWorker:
			olog(fmt.Sprintf("%d Received kill command", identity), config.DebugMode)
			return
		case d, ok := <-msgsGcm:
			if !ok {
				// If channel is closed then wait for it to be reconnected or wait for kill signal
				time.Sleep(100 * time.Millisecond)
				continue
			}
			olog(fmt.Sprintf("%d Worker Received a message: %s", identity, d.Body), config.DebugMode)

			// GCM work
			payload := Message{}
			err := json.Unmarshal(d.Body, &payload)
			if err != nil {
				logger.Printf("Unmarshal error = %s, for MQ message data = %s", err.Error(), d.Body)
				olog(fmt.Sprintf("Unmarshal error = %s, for MQ message data = %s", err.Error(), d.Body), config.DebugMode)
			} else {
				data := payload.Body
				regIDs := payload.Token

				msg := gcm.NewMessage(data, regIDs...)
				response, err := sender.Send(msg, 2)
				if err != nil {
					//					gcmErrCount++
					logger.Printf("GCM send error = %s, data=%s", err.Error())
					olog(fmt.Sprintf("GCM send error = %s", err.Error()), config.DebugMode)

					// In case of GCM error / Requeue and continue to next
					// TODO: Inform admin

					// Same error continues without break for 10 times, sleep for a minute
					// TODO: Implement this in a better way
					//					if gcmErrCount >= 10 {
					//						time.Sleep(time.Minute)
					//					}

					d.Nack(false, true)
					continue
				} else {
					//					gcmErrCount = 0
					// TODO: Implement this as proper goroutine, its dangereous if user exists before this goroutine ends
					// Running result processor as a goroutine so that current worker can proceed with sending another GCM request to
					// server, without getting delayed by processing
					go func() {
						olog(fmt.Sprintf("%s", response.Results), config.DebugMode)
						for i, result := range response.Results {
							olog("one", config.DebugMode)
							isSentToClientSuccesfully := StatusErrGcmError
							t := time.Now()
							ts := t.Format(time.RFC3339)

							if result.Error == "NotRegistered" || result.Error == "InvalidRegistration" {
								statusInactiveMsg := GcmStatusInactiveMsg{Token: payload.Token[i]}
								jsonStatusInactiveMsg, err := json.Marshal(statusInactiveMsg)
								if err != nil {
									logger.Printf("GCM status inactive Marshal error = %s", err.Error())
									olog(fmt.Sprintf("GCM status inactive Marshal error = %s", err.Error()), config.DebugMode)
								}

								err = ch.Publish(
									"", // exchange
									GcmStatusInactiveQueueName, // routing key
									false, // mandatory
									false,
									amqp.Publishing{
										DeliveryMode: amqp.Persistent,
										ContentType:  "text/json",
										Body:         jsonStatusInactiveMsg,
									})
								if err != nil {
									logger.Printf("GCM status inactive Publish error = %s", err.Error())
									olog(fmt.Sprintf("GCM status inactive Publish error = %s", err.Error()), config.DebugMode)
								}

							} else if result.RegistrationID != "" && result.MessageID != "" {
								// Sent succesfully but google reported that GCM id has been updated

								// Send to Queue -> gcm_token_update
								tokenUpdateMsg := GcmTokenUpdateMsg{OldToken: payload.Token[i], NewToken: result.RegistrationID}
								jsonTokenUpdateMsg, err := json.Marshal(tokenUpdateMsg)
								if err != nil {
									logger.Printf("GCM RegistrationID update error = %s", err.Error())
									olog(fmt.Sprintf("GCM RegistrationID update error = %s", err.Error()), config.DebugMode)
								}

								err = ch.Publish(
									"", // exchange
									GcmTokenUpdateQueueName, // routing key
									false, // mandatory
									false,
									amqp.Publishing{
										DeliveryMode: amqp.Persistent,
										ContentType:  "text/json",
										Body:         jsonTokenUpdateMsg,
									})
								if err != nil {
									logger.Printf("GCM RegistrationID update error = %s", err.Error())
									olog(fmt.Sprintf("GCM RegistrationID update error = %s", err.Error()), config.DebugMode)
								}

								// Sent succesfully
								isSentToClientSuccesfully = StatusSuccessGcmRequest
							} else if result.Error == "DeviceMessageRateExceeded" {
								// The rate of messages to a particular device is too high. Reduce the number of messages sent to
								// this device and do not immediately retry sending to this device.
								// Todo: Maybe send negative acknowledgement or move to another queue
							} else if result.Error == "TopicsMessageRateExceeded" {
								// The rate of messages to subscribers to a particular topic is too high. Reduce the number of messages
								// sent for this topic, and do not immediately retry sending.
								// Todo: Maybe send negative acknowledgement or move to another queue
							} else if result.Error != "" {
								// TODO: Do some special procesing for these things
							} else {
								// Success
								isSentToClientSuccesfully = StatusSuccessGcmRequest
							}

							gcmLog, err := json.Marshal(GcmLog{TimeStamp: ts, Type: isSentToClientSuccesfully, GcmId: payload.Token[i], Data: GcmError{Result: result, MulticastId: response.MulticastID}})
							if err != nil {
								logger.Printf("Marshal error while logging GCM response = %s", err.Error())
								olog(fmt.Sprintf("Marshal error while logging GCM response = %s", err.Error()), config.DebugMode)
							}
							ch_gcm_err <- gcmLog
						}
					}()
				}
			}
			// Acknowledge to MQ that work has been processed successfully
			d.Ack(false)
		}
	}
}

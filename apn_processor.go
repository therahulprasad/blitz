package main

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
	"github.com/sideshow/apns2/certificate"
	apns "github.com/sideshow/apns2"
	"strconv"
)

/**
 * An APN Worker which reads from RabbitMQ queue and sends APN messages
 * If APN is expired or invalid it sends APN key to a queue which is processed by another goroutine and updated in database
 */
func apn_processor(identity int, config Configuration, conn *amqp.Connection,
	ApnStatusInactiveQueueName, ApnQueueName string, ch_err, ch_apn_log_success chan []byte, logger *log.Logger,
	killWorker chan int, apnQueue ApnQueue) {
	isHourly := apnQueue.IsHourly
	pemPath := apnQueue.PemPath
	apnTopic := apnQueue.Topic
	// Load PEM file specified in config file required to send APN messages
	cert, pemErr := certificate.FromPemFile(pemPath, "")
	failOnError(pemErr, "Failed to load PEM file for APN")

	ApnQueueNameOriginal := ApnQueueName
	now := time.Now()
	var hourlyTick <-chan time.Time
	if isHourly == true {
		curHour := ""
		curHour  = strconv.Itoa(now.Hour())
		curHourInt := now.Hour()
		if (curHourInt < 10) {
			curHour = "0" + strconv.Itoa(curHourInt)
		} else {
			curHour = strconv.Itoa(curHourInt)
		}
		//tmp := now.Second()%24
		//if tmp < 10 {
		//	curHour = "0" + strconv.Itoa(tmp)
		//} else {
		//	curHour = strconv.Itoa(tmp)
		//}
		ApnQueueName = ApnQueueNameOriginal + "_" + curHour
		waitTime := 60 - now.Minute()
		hourlyTick = time.After(time.Duration(waitTime * 61) * time.Second)
		//fmt.Println(waitTime%24)
		//hourlyTick = time.After(time.Duration(waitTime%24) * time.Second)
		olog(fmt.Sprintf("Connecting to " + ApnQueueName), config.DebugMode)
	}

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

	msgsApn, err := ch.Consume(
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
		case <-hourlyTick:
			olog(fmt.Sprintf("Ticking"), config.DebugMode)
			curHour := ""
			now = time.Now()
			curHourInt := now.Hour()
			if (curHourInt < 10) {
				curHour = "0" + strconv.Itoa(curHourInt)
			} else {
				curHour = strconv.Itoa(curHourInt)
			}
			ch.Cancel(ApnQueueName, false)
			ApnQueueName = ApnQueueNameOriginal + "_" + curHour
			msgsApn, err = ch.Consume(
				ApnQueueName, // queue
				"",           // consumer
				false,        // auto-ack
				false,        // exclusive
				false,        // no-local
				false,        // no-wait
				nil,          // args
			)
			failOnError(err, "Failed to register a consumer")
			olog(fmt.Sprintf("Connected to " + ApnQueueName), config.DebugMode)

			waitTime := 60 - now.Minute()
			hourlyTick = time.After(time.Duration(waitTime * 61) * time.Second)
		case <-killWorker:
		// If a message is receieved through killWorker then log and exit
		olog(fmt.Sprintf("%d Received kill command", identity), config.DebugMode)
		return

		case d, ok := <-msgsApn:
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

		// If config contains time to love for message then add it
		if apnQueue.TtlSeconds > 0 {
			notification.Expiration = time.Now().Add(time.Duration(apnQueue.TtlSeconds) * time.Second)
		}

		// If json data contains ttl value then override it
		if payload.TimeToLiveSeconds > 0 {
			notification.Expiration = time.Now().Add(time.Duration(payload.TimeToLiveSeconds) * time.Second)
		}

		// Could not encode map to json.
		if err != nil {
			logger.Printf("Marshal error = %s, for MQ message data = %s", err.Error(), d.Body)
			olog(fmt.Sprintf("Marshal error = %s, for MQ message data = %s", err.Error(), d.Body), config.DebugMode)
			d.Ack(false)
			continue
		}

		res, err := client.Push(notification)
		if err != nil {
			// gcmErrCount++
			notificationByteArray, _ := json.Marshal(notification)
			logger.Printf("APN send error = %s, data=%s", err.Error(), notificationByteArray)
			olog(fmt.Sprintf("APN send error = %s", err.Error()), config.DebugMode)

			// In case of APN error / Requeue and continue to next
			// TODO: Inform admin

			// Same error continues without break for 10 times, sleep for a minute
			// TODO: Implement this in a better way
			//					if gcmErrCount >= 10 {
			//						time.Sleep(time.Minute)
			//					}

			key := token
			val, ok := retries_apn.Get(key)
			var valint int
			if ok {
				valint, ok = val.(int)

				if ok {
					valint = valint + 1
				} else {
					// Log error (This should not happen) and continue
					logger.Printf("APN Could not convert interface to int = %v", val)
					olog(fmt.Sprintf("APN Could not convert interface to int = %v", val), config.DebugMode)

					d.Ack(false)
					continue
				}
			} else {
				valint = 1
			}

			// If already tried 5 times, then do not requeue
			if valint >= config.APN.RequeueCount+1 {
				// Log it and continue
				logger.Printf("Max Retries APN send data=%s", notificationByteArray)
				olog(fmt.Sprintf("Max Retries APN send error. Data = %s", notificationByteArray), config.DebugMode)

				retries_apn.Remove(key)
				d.Ack(false)
				continue
			}

			retries_apn.Set(key, valint)
			d.Nack(false, true)

			continue
		} else {
			// gcmErrCount = 0
			// TODO: Implement this as proper goroutine, its dangereous if user exists before this goroutine ends
			// Running result processor as a goroutine so that current worker can proceed with sending another APN request to
			// server, without getting delayed by processing
			go func() {
				isSentToClientSuccesfully := StatusErrApnError
				t := time.Now()
				ts := t.Format(time.RFC3339)

				if res.StatusCode == 200 {
					// Success (Log to file at bottom)
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
					// Error is logged automatically after this block
				} else if (res.Reason == "TooManyRequests") {
					// TODO: Delay sending instead of skipping
					// Error is logged automatically after this block
				}

				apnLog, err := json.Marshal(ApnLog{TimeStamp: ts, Type: isSentToClientSuccesfully, ApnId: token, Data: ApnError{Reason:res.Reason}})
				if err != nil {
					logger.Printf("Marshal error while logging APN response = %s", err.Error())
					olog(fmt.Sprintf("Marshal error while logging APN response = %s", err.Error()), config.DebugMode)
				}

				if isSentToClientSuccesfully == StatusSuccessApnRequest {
					ch_apn_log_success <- apnLog
				} else {
					ch_err <- apnLog
				}
			}()
		}
		// Acknowledge to MQ that work has been processed successfully
		d.Ack(false)
		}
	}
}

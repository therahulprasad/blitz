package main
import (
	"fmt"
	"encoding/json"
	"github.com/ziutek/mymysql/autorc"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
	"time"
)

// TODO: Try for at least 3 times before discarding a message (+ What happens if new field is added to MQ json ?)
// DONE: Implement kill channel for the goroutine
func apn_error_processor_status_inactive(config Configuration, conn *amqp.Connection, ApnStatusInactiveQueueName string,
ch_custom_err chan []byte, logger *log.Logger, killApnStatusInactive, killApnStatusInactiveAck chan int, gq ApnQueue) {
	// Connect to a database
	db := autorc.New("tcp", "", config.Db.DbHost+":"+strconv.Itoa(config.Db.DbPort), config.Db.DbUser, config.Db.DbPassword, config.Db.DbDatabase)

	var upd autorc.Stmt
	err := db.PrepareOnce(&upd, gq.Queries.StatusInactive)
	if err != nil {
		failOnError(err, "Could not create prepared statement")
	}

	// Create new channel for Status Inactive
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	// Connect to ApnStatusInactiveQueueName channel as consumer
	msgsStatusInactive, err := ch.Consume(
		ApnStatusInactiveQueueName, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	i := 0
	payloads := make([]ApnStatusInactiveMsg, config.Db.TransactionMinCount.StatusInactive)
	for  {
		select {
		case d, ok := <-msgsStatusInactive:
			if !ok {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			olog(fmt.Sprintf("APN Status Inactive Received a message: %s", d.Body), config.DebugMode)

			payload  := ApnStatusInactiveMsg{}
			err := json.Unmarshal(d.Body, &payload)

			if err != nil {
				logger.Printf("Unmarshal error for APN status Inactive MQ message data = %s",d.Body)
				olog(fmt.Sprintf("Unmarshal error for APN Token Update MQ message data = %s",d.Body), config.DebugMode)
			} else {
				payloads[i] = payload
				i++

				if i == config.Db.TransactionMinCount.StatusInactive {
					i = 0
					err := db.Begin(func(tr mysql.Transaction, args ...interface{}) error {
						for _, v := range payloads {
							if v.Token != "" {
								_, err := tr.Do(upd.Raw).Run(v.Token)
								if err != nil {
									return err
								}
							}
						}
						return tr.Commit()
					})
					t := time.Now()
					ts := t.Format(time.RFC3339)

					if err != nil {
						// ERROR WHILE UPDATING DB

						olog("Database Transaction Error StatusErrStatusInactiveTransaction", config.DebugMode)

						errInfo := make(map[string]interface{})
						errInfo["error"] = err.Error()
						errInfo["payloads"] = payloads
						errLog := DbLog{TimeStamp:ts, Type:StatusErrStatusInactiveTransaction, Data:errInfo}

						errLogByte, err := json.Marshal(errLog)
						if err == nil {
							ch_custom_err <- errLogByte
						} else {
							logger.Printf("Marshal error for ErrStatusInactiveTransaction")
						}
					} else {
						// SUCCESSFULLY UPDATED
						olog("Database Transaction Success StatusSuccessStatusInactiveTransaction", config.DebugMode)
						errLog := DbLog{TimeStamp:ts, Type:StatusSuccessStatusInactiveTransaction, Data:payloads}

						errLogByte, err := json.Marshal(errLog)
						if err == nil {
							ch_custom_err <- errLogByte
						} else {
							logger.Printf("Marshal error for StatusErrStatusInactiveTransaction")
						}
					}
					// For for specified time before running next query
					time.Sleep(time.Duration(config.Db.WaitTimeMs.StatusInactive) * time.Millisecond)
				}
			}
			// Acknowledge to MQ that work has been processed successfully
			d.Ack(false)
		case ack := <-killApnStatusInactive:
			olog("Killing APN Status Inactive goroutine", config.DebugMode)
			// Write to database and exit from goroutine
			if i > 0 {
				i = 0
				err := db.Begin(func(tr mysql.Transaction, args ...interface{}) error {
					for _, v := range payloads {
						if v.Token != "" {
							_, err := tr.Do(upd.Raw).Run(v.Token)
							if err != nil {
								return err
							}
						}
					}
					return tr.Commit()
				})
				if err != nil {
					olog("Database Transaction Error while exiting + StatusErrStatusInactiveTransaction", config.DebugMode)
					t := time.Now()
					ts := t.Format(time.RFC3339)
					errInfo := make(map[string]interface{})
					errInfo["error"] = err.Error()
					errInfo["payloads"] = payloads
					errLog := DbLog{TimeStamp:ts, Type:StatusErrStatusInactiveTransaction, Data:errInfo}
					errLogByte, err := json.Marshal(errLog)
					if err == nil {
						ch_custom_err <- errLogByte
					} else {
						logger.Printf("Marshal error for ErrStatusInactiveTransaction while quiting")
					}
				}
			}
			if ack == NeedAck {
				killApnStatusInactiveAck<- 1
			}

			olog("APN status inactive goroutine killed", config.DebugMode)
			// Exit from goroutine
			return
		}
	}
}

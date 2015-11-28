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
)

// TODO: Try for at least 3 times before discarding a message (+ What happens if new field is added to MQ json ?)
// DONE: Implement kill channel for the goroutine
func gcm_error_processor_status_inactive(config Configuration, conn *amqp.Connection, GcmStatusInactiveQueueName string, ch_custom_err chan []byte, logger *log.Logger, killStatusInactive, killStatusInactiveAck chan int) {
	// Connect to a database
	db := autorc.New("tcp", "", config.Db.DbHost+":"+strconv.Itoa(config.Db.DbPort), config.Db.DbUser, config.Db.DbPassword, config.Db.DbDatabase)

	var upd autorc.Stmt
	err := db.PrepareOnce(&upd, config.Db.Queries.StatusInactive)
	if err != nil {
		failOnError(err, "Could not create prepared statement")
	}
	//	upd, _ := db.Prepare("UPDATE bobble_user_gcm SET gcm_id = ?, gcm_status = 1 WHERE gcm_id = ?")

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

	msgsStatusInactive, err := ch.Consume(
		GcmStatusInactiveQueueName, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	i := 0
	payloads := make([]GcmStatusInactiveMsg, config.Db.TransactionMinCount.StatusInactive)
	for  {
		select {
		case d := <-msgsStatusInactive:
			olog(fmt.Sprintf("Received a message: %s", d.Body), config.DebugMode)

			payload  := GcmStatusInactiveMsg{}
			err := json.Unmarshal(d.Body, &payload)

			if err != nil {
				logger.Printf("Unmarshal error for status Inactive MQ message data = %s",d.Body)
				olog(fmt.Sprintf("Unmarshal error for Token Update MQ message data = %s",d.Body), config.DebugMode)
			} else {
				payloads[i] = payload
				i++

				if i == config.Db.TransactionMinCount.StatusInactive {
					i = 0
					err := db.Begin(func(tr mysql.Transaction, args ...interface{}) error {
						for _, v := range payloads {
							_, err := tr.Do(upd.Raw).Run(v.Token)
							if err != nil {
								return err
							}
						}
						return tr.Commit()
					})
					if err != nil {
						olog("Database Transaction Error ErrStatusInactiveTransaction", config.DebugMode)
						errLog := CustomErrorLog{Type:ErrStatusInactiveTransaction, Data:payloads}
						errLogByte, err := json.Marshal(errLog)
						if err == nil {
							ch_custom_err <- errLogByte
						} else {
							logger.Printf("Marshal error for ErrStatusInactiveTransaction")
						}
					}
				}
			}
			// Acknowledge to MQ that work has been processed successfully
			d.Ack(false)
		case ack := <-killStatusInactive:
			olog("Killing inactive goroutine", config.DebugMode)
			// Write to database and exit from goroutine
			if i > 0 {
				i = 0
				err := db.Begin(func(tr mysql.Transaction, args ...interface{}) error {
					for _, v := range payloads {
						_, err := tr.Do(upd.Raw).Run(v.Token)
						if err != nil {
							return err
						}
					}
					return tr.Commit()
				})
				if err != nil {
					olog("Database Transaction Error while exiting + ErrStatusInactiveTransaction", config.DebugMode)
					errLog := CustomErrorLog{Type:ErrStatusInactiveTransaction, Data:payloads}
					errLogByte, err := json.Marshal(errLog)
					if err == nil {
						ch_custom_err <- errLogByte
					} else {
						logger.Printf("Marshal error for ErrStatusInactiveTransaction while quiting")
					}
				}
			}
			if ack == NeedAck {
				killStatusInactiveAck<- 1
			}
			// Exit from goroutine
			return
		}
	}
}

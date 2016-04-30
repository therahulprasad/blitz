package main

import "github.com/streadway/amqp"

func createQueues(config Configuration, ch *amqp.Channel) {
	if config.Rabbit.CreateQueues {
		// Create Queues for GCM from config
		for i:=0; i < len(config.GcmQueues); i++ {
			_, err := ch.QueueDeclare(
				config.GcmQueues[i].Name, // name
				true,         // durable
				false,        // delete when unused
				false,        // exclusive
				false,        // no-wait
				nil,          // arguments
			)
			failOnError(err, "Failed to declare a queue")

			_, err = ch.QueueDeclare(
				config.GcmQueues[i].GcmTokenUpdateQueue, // name
				true,         // durable
				false,        // delete when unused
				false,        // exclusive
				false,        // no-wait
				nil,          // arguments
			)
			failOnError(err, "Failed to declare a queue")

			_, err = ch.QueueDeclare(
				config.GcmQueues[i].GcmStatusInactiveQueue, // name
				true,         // durable
				false,        // delete when unused
				false,        // exclusive
				false,        // no-wait
				nil,          // arguments
			)
			failOnError(err, "Failed to declare a queue")
		}

		// Create Queues for APN from config
		for i:=0; i< len(config.ApnQueues); i++ {
			_, err := ch.QueueDeclare(
				config.ApnQueues[i].Name, // name
				true,         // durable
				false,        // delete when unused
				false,        // exclusive
				false,        // no-wait
				nil,          // arguments
			)
			failOnError(err, "Failed to declare a queue")

			_, err = ch.QueueDeclare(
				config.ApnQueues[i].ApnStatusInactiveQueue, // name
				true,         // durable
				false,        // delete when unused
				false,        // exclusive
				false,        // no-wait
				nil,          // arguments
			)
			failOnError(err, "Failed to declare a queue")
		}

		err := ch.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		failOnError(err, "Failed to set QoS")
	}
}


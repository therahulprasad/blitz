# blitz #
__v0.2.2__
GCM and APN logging bug fixed. If push fails, data was missing from logs. 

__v0.2.1__
APN logging bug fixed

__v0.2__
This program can run a set of workers to send GCM (Android Notification) / APN (iOS notification) messages concurrently. It reads messages from RabbitMQ and process it.  

Blitz uses go's concurrency model to dispatch GCM and APN messages with high throughput.
It uses rabbitMq as message queue to fetch notification data to be sent.
Message should be a json of following format.  
     {
         "Token": "xxxxxx",
         "Body" : {
             // Json body to be sent as notification
         }
     }


## Dependencies ##
1. Rabbit MQ
1. MySql (optional) for updating status of GCM/apn token

## How to use ##
1. Rename config.sample.json to config.json and update required fields
2. Run executable
3. That's it

## Configuration ##
    {
      // Number of workers to run at a time, -ve value means 1
      "NumWorkers": -1, 
      
      // Will print debug text in screen, it should be false when running as daemon
      "DebugMode" : true,
    
      // Rabbit MQ configuration
      "Rabbit"    : {
        "Username": "guest",
        "Password": "guest",
        "Host"    : "localhost",
        "Port"    : 5672,
        "Vhost"   : "bobble",
                
        // Seconds to wait before reconnecting (If disconnected)
        "ReconnectWaitTimeSec" : 5,
        
        // Name of the message queue to fetch GCM messages 
        "GcmMsgQueue": "gcm_messages",
        
        // Name of the queue to send Token Update error (Which can be later processed and token can be updated)
        "GcmTokenUpdateQueue" : "gcm_token_update",
        
        // Name of teh queue to send error such as NotRegistered or InvalidToken (Which can be processed later)
        "GcmStatusInactiveQueue" : "gcm_status_inactive",
        
        // Attempt to declare queues if not present
        "CreateQueues" : true
      },
      "ApnQueues": [
          {
            "Identifier": "primary",
            "Name": "apn_messages",
            "NumWorkers": 10,
            "PemPath": "",
            "ApnStatusInactiveQueue": "apn_status_inactive",
            "Topic": "com.blah.blah",
            "Queries": {
              "StatusInactive":"UPDATE table_name SET apn_status = '-1' WHERE apn_key LIKE ?"
            }
          }
      ],
      "GCM"       : {
        // API ket for GCM
        "ApiKey"  : "your_api_key"
      },
      "Logging"   : {
        // GCM Error log will be stored as json in date separated, hourly files
        "GcmErr"   : {
          "RootPath" : "/Users/rahulprasad/Documents/go/src/github.com/touchtalent/GoWorkerGCM/log".
          
          // If true, it will log Successful GCM calls
          "LogSuccess": true
        },
        // Plain text log for debugging errors occured within app, such as error while decoding json
        "AppErr"  : {
          "FilePath" : "/Users/rahulprasad/Documents/go/src/github.com/touchtalent/GoWorkerGCM/app-err.log"
        }
      },
        // Database configurations
      "Db" : {
        "DbHost" : "127.0.0.1",
        "DbPort" : 3306,
        "DbUser" : "root",
        "DbPassword" : "root",
        "DbDatabase" : "bobble_local",
        
        // Number of messages to wait for before running transaction 
        "TransactionMinCount" : {
         "TokenUpdate" : 2,
         "StatusInactive" : 2
        },
        
        // Queries to run for each GCM Error message 
        "Queries": {
         // For TokenUpdate first ? will be replaced by NewToken and 2nd ? will be replaced by OldToken
         "TokenUpdate":"UPDATE user_gcm SET gcm_id = ? WHERE gcm_id = ?",
         
         // For StatusInactive ? will be replaced by Token
         "StatusInactive":"UPDATE user_gcm SET gcm_status = -1 WHERE gcm_id = ?"
        }
      }
    }
    
## Todo ##
1. Create an http server for instant GCM delivery
2. In case of error Try sending multiple times before discarding a message
3. Requeue (GCM error or network failure) specific number of times and then discard
4. Implement Better strructure for app error
5. Implement priority queue
6. Implement logger as a separate module
7. Write Test cases 

## Known Bugs ##
1. Sometimes APN/2 library gets stucks while sending notification. Quiting at that time waits for library to complete transaction, which sometimes takes too long. Try to kill it instead.
 
## Change Log ##
0.2
---
Implemeted support for APN/2

0.1
---
Implemented support for GCM
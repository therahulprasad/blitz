package main
import (
	"github.com/therahulprasad/go-fcm"
)

type GcmError struct {
	Result gcm.Result
	MulticastId int64
}
type ApnError struct {
	Reason string
}
type GcmLog struct {
	TimeStamp string
	Type string
	GcmId string
	Data interface{}
}
type ApnLog struct {
	TimeStamp string
	Type string
	ApnId string
	Data interface{}
}
type DbLog struct {
	TimeStamp string
	Type string
	Data interface{}
}

type GcmTokenUpdateMsg struct {
	OldToken string
	NewToken string
}
type GcmStatusInactiveMsg struct {
	Token string
}
type ApnStatusInactiveMsg struct {
	Token string
}
type Configuration struct {
	DebugMode bool `json:"DebugMode"`
	SingularityPort string `json:"SingularityPort"`
	Rabbit struct {
				   Username string `json:"Username"`
				   Password string `json:"Password"`
				   Host string `json:"Host"`
				   Port int `json:"Port"`
				   Vhost string `json:"Vhost"`
				   ReconnectWaitTimeSec int `json:"ReconnectWaitTimeSec"`
				   CreateQueues bool `json:"CreateQueues"`
			   } `json:"Rabbit"`
	GcmQueues []GcmQueue `json:"GcmQueues"`
	ApnQueues []ApnQueue `json:"ApnQueues"`
	GCM struct {
				   ApiKey	string `json:"ApiKey"`
				   RequeueCount	int `json:"RequeueCount"`
			   } `json:"GCM"`
	APN struct {
		    RequeueCount	int `json:"RequeueCount"`
	    } `json:"APN"`
	Logging struct {
				GcmErr struct {
						   RootPath string `json:"RootPath"`
						   SuccessPath string `json:"SuccessPath"`
						   LogSuccess bool `json:"LogSuccess"`
					   } `json:"GcmErr"`
				ApnErr struct {
					       RootPath string `json:"RootPath"`
					       SuccessPath string `json:"SuccessPath"`
					       LogSuccess bool `json:"LogSuccess"`
				       } `json:"ApnErr"`
				DbErr struct {
						   RootPath string `json:"RootPath"`
						   LogSuccess bool `json:"LogSuccess"`
					   } `json:"DbErr"`
				AppErr struct {
						   FilePath string `json:"FilePath"`
					   } `json:"AppErr"`
			   } `json:"Logging"`
	Db struct {
		   DbHost string `json:"DbHost"`
		   DbPort int `json:"DbPort"`
		   DbUser string `json:"DbUser"`
		   DbPassword string `json:"DbPassword"`
		   DbDatabase string `json:"DbDatabase"`
		   TransactionMinCount struct {
					  TokenUpdate int `json:"TokenUpdate"`
					  StatusInactive int `json:"StatusInactive"`
				  } `json:"TransactionMinCount"`
		   WaitTimeMs struct {
						  TokenUpdate int `json:"TokenUpdate"`
						  StatusInactive int `json:"StatusInactive"`
					  } `json:"WaitTimeMs"`
	   } `json:"Db"`
}

type Message struct {
	Token []string `json:"Token"`
	Body map[string]interface{} `json:"Body"`
}
type ApnMessage struct {
	Token string `json:"Token"`
	Body map[string]interface{} `json:"Body"`
}

type GcmQueue struct {
	Identifier string `json:"identifier"`
	Name string `json:"Name"`
	Numworkers int `json:"NumWorkers"`
	ApiKey string `json:"ApiKey"`
	GcmTokenUpdateQueue string `json:"GcmTokenUpdateQueue"`
	GcmStatusInactiveQueue string `json:"GcmStatusInactiveQueue"`
	Queries struct {
				TokenUpdate string `json:"TokenUpdate"`
				StatusInactive string `json:"StatusInactive"`
			} `json:"Queries"`
	IsHourly bool `json:"isHourly"`
}
type ApnQueue struct {
	Identifier string `json:"Identifier"`
	Name string `json:"Name"`
	NumWorkers int `json:"NumWorkers"`
	PemPath string `json:"PemPath"`
	Topic string `json:"Topic"`
	ApnStatusInactiveQueue string `json:"ApnStatusInactiveQueue"`
	Queries struct {
			   StatusInactive string `json:"StatusInactive"`
		   } `json:"Queries"`
	IsHourly bool `json:"isHourly"`
}


const NeedAck = 1
const NoAckNeeded = 2

// TODO: Make configurable
// TODO: Refactor these consts
const StatusErrTokenUpdateTransaction = "ErrTokenUpdateTransaction"
const StatusErrStatusInactiveTransaction = "ErrStatusInactiveTransaction"
const StatusErrGcmError = "ErrGcmError"
const StatusSuccessGcmRequest = "SuccessGcm"
const StatusSuccessApnRequest = "SuccessApn"
const StatusErrApnError = "ErrorApn"
const StatusSuccessTokenUpdateTransaction = "SuccessTokenUpdateTransaction"
const StatusSuccessStatusInactiveTransaction = "SuccessStatusInactiveTransaction"
package main
import (
	"github.com/alexjlockwood/gcm"
)

type GcmError struct {
	Result gcm.Result
	OldToken string
	MulticastId int64
}

type GcmTokenUpdateMsg struct {
	OldToken string
	NewToken string
}
type GcmStatusInactiveMsg struct {
	Token string
}
type Configuration struct {
	NumWorkers int `json:"NumWorkers"`
	DebugMode bool `json:"DebugMode"`
	Rabbit struct {
				   Username string `json:"Username"`
				   Password string `json:"Password"`
				   Host string `json:"Host"`
				   Port int `json:"Port"`
				   Vhost string `json:"Vhost"`
				   GcmMsgQueue string `json:"GcmMsgQueue"`
				   GcmTokenUpdateQueue string `json:"GcmTokenUpdateQueue"`
				   GcmStatusInactiveQueue string `json:"GcmStatusInactiveQueue"`
			   } `json:"Rabbit"`
	GCM struct {
				   ApiKey	string `json:"ApiKey"`
			   } `json:"GCM"`
	Logging struct {
				GcmErr struct {
						   RootPath string `json:"RootPath"`
					   } `json:"GcmErr"`
				AppErr struct {
						   FilePath string `json:"FilePath"`
					   } `json:"AppErr"`
			   } `json:"Logging"`
}

type Message struct {
	Token []string `json:"Token"`
	Body map[string]interface{} `json:"Body"`
}
package main
import (
	"fmt"
	"os"
	"strconv"
	"time"
	"log"
)

func logErrToFile(rootpath string, errInfo chan []byte, debugmode bool) {
	errFileInfo := ErrFileInfo{}
	newline := []byte("\n")
	var fp *os.File

	// For All Gcm Error sent log it into a file
	for e := range errInfo {
		t := time.Now()
		dateFolder := t.Format("2006-01-02")
		hourFolder := strconv.Itoa(t.Hour())
		if errFileInfo.file == nil || (errFileInfo.date != "" && dateFolder != errFileInfo.date) || (errFileInfo.hour != "" && hourFolder != errFileInfo.hour) {
			basepath := rootpath + "/" + dateFolder
			if _, err := os.Stat(basepath); os.IsNotExist(err) {
				err := os.Mkdir(basepath, os.FileMode(int(0777)))
				if err != nil {
					failOnError(err, "Could not create directory : " + basepath)
				}
			}

			fpath := basepath + "/" + "hourly-" + hourFolder
			hostname, err := os.Hostname()
			if err == nil {
				fpath = fpath + "-" + hostname
			}
			fpath = fpath + ".log"

			// Create new file if it does not exists
			fp, err = os.OpenFile(fpath, os.O_APPEND | os.O_CREATE | os.O_WRONLY, os.FileMode(int(0777)))
			defer fp.Close()
			if err == nil {
				errFileInfo.file = fp
				errFileInfo.date = dateFolder
				errFileInfo.hour = hourFolder

				dWrite := e
				_, err := fp.Write(append(dWrite[:], newline[:]...))
				fp.Sync()
				if err != nil {
					olog(fmt.Sprintf("Error occured while writing + " + err.Error() + " Payload: %s", dWrite), debugmode)
				}
			} else {
				failOnError(err, "Could not create/open log file : " + fpath)
			}
		} else {
			dWrite := e
			_, err := fp.Write(append(dWrite[:], newline[:]...))
			fp.Sync()
			if err != nil {
				olog(fmt.Sprintf("Error occured while writing + " + err.Error() + " Payload: %s", dWrite), debugmode)
			}
		}
	}
}


func olog(str string, debugmode bool) {
	if debugmode {
		log.Println(str)
	}
}


type ErrFileInfo struct {
	file *os.File
	date string
	hour string
}
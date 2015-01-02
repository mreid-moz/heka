/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

package s3splitfile

import (
	"errors"
	"fmt"
	"strings"
	. "github.com/mozilla-services/heka/pipeline"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type SplitFileInfo struct {
	name string
	lastUpdate time.Time
}

// Output plugin that writes message contents to a file on the file system.
type S3SplitFileOutput struct {
	*S3SplitFileOutputConfig
	perm       os.FileMode
	folderPerm os.FileMode
	timerChan  <-chan time.Time
	dimFiles   map[string]*SplitFileInfo
}

// ConfigStruct for FileOutput plugin.
type S3SplitFileOutputConfig struct {
	// Base output file path.
	// In-progress files go to <Path>/current/<dimensionPath>,
	// finalized files go to <Path>/finalized/<dimensionPath>
	Path string

	// Output file permissions (default "644").
	Perm string

	// Interval at which we should check MaxFileAge for in-flight files.
	FlushInterval uint32 `toml:"flush_interval"`

	// Permissions to apply to directories created for output directories if
	// they don't exist.  Must be a string representation of an octal integer.
	// Defaults to "700".
	FolderPerm string `toml:"folder_perm"`

	// Specifies whether or not Heka's stream framing will be applied to the
	// output. We do some magic to default to true if ProtobufEncoder is used,
	// false otherwise.
	UseFraming *bool `toml:"use_framing"`

	// Specifies how much data (in bytes) can be written to a single file before
	// we rotate and begin writing to another one (default 500 * 1024 * 1024,
	// i.e. 500MB).
	MaxFileSize uint32 `toml:"max_file_size"`

	// Specifies how long (in milliseconds) to wait before rotating the current
	// file and begin writing to another one (default 60 * 60 * 1000, i.e. 1hr).
	MaxFileAge uint32 `toml:"max_file_age"`
}

// TODO: get these from a spec file instead of hard-coding.
var acceptableChannels = map[string]bool{
	"default": true,
	"nightly": true,
	"aurora": true,
	"beta": true,
	"release": true,
	"esr": true,
}

var hostname, _ = os.Hostname()

// Pattern to use for sanitizing path/file components.
var cleanPattern = regexp.MustCompile("[^a-zA-Z0-9_/.]")

// Names for the subdirectories to use for in-flight and finalized files. These
// dirs are found under the main Path specified in the config.
const (
	stdCurrentDir = "current"
	stdFinalizedDir = "finalized"
)

func (o *S3SplitFileOutput) ConfigStruct() interface{} {
	return &S3SplitFileOutputConfig{
		Perm:          "644",
		FlushInterval: 1000,
		FolderPerm:    "700",
		MaxFileSize:   524288000,
		MaxFileAge:    3600000,
	}
}

func (o *S3SplitFileOutput) Init(config interface{}) (err error) {
	conf := config.(*S3SplitFileOutputConfig)
	o.S3SplitFileOutputConfig = conf
	var intPerm int64

	if intPerm, err = strconv.ParseInt(conf.FolderPerm, 8, 32); err != nil {
		err = fmt.Errorf("S3SplitFileOutput '%s' can't parse `folder_perm`, is it an octal integer string?",
			o.Path)
		return
	}
	o.folderPerm = os.FileMode(intPerm)

	if intPerm, err = strconv.ParseInt(conf.Perm, 8, 32); err != nil {
		err = fmt.Errorf("S3SplitFileOutput '%s' can't parse `perm`, is it an octal integer string?",
			o.Path)
		return
	}
	o.perm = os.FileMode(intPerm)

	if conf.MaxFileSize < 1 {
		err = fmt.Errorf("Parameter 'max_file_size' must be greater than 0.")
		return
	}
	if conf.MaxFileAge < 1 {
		err = fmt.Errorf("Parameter 'max_file_age' must be greater than 0.")
		return
	}

	o.dimFiles = map[string]*SplitFileInfo{}
	return
}

func (o *S3SplitFileOutput) writeMessage(fileSuffix string, msgBytes []byte) (rotate bool, err error) {
	rotate = false
	fileName := o.getCurrentFileName(fileSuffix)
	filePath := filepath.Dir(fileName)

	if e := os.MkdirAll(filePath, o.folderPerm); e != nil {
		return rotate, fmt.Errorf("S3SplitFileOutput can't create file path '%s': %s", filePath, e)
	}

	f, e := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, o.perm)
	if e != nil {
		return rotate, e
	}
	defer f.Close()

	n, e := f.Write(msgBytes)
	if e != nil {
		return rotate, fmt.Errorf("Can't write to %s: %s", fileName, e)
	} else if n != len(msgBytes) {
		return rotate, fmt.Errorf("Truncated output for %s", fileName)
	} else {
		f.Sync()

		if fi, e := f.Stat(); e != nil {
			return rotate, e
		} else {
			if fi.Size() >= int64(o.MaxFileSize) {
				rotate = true
			}
		}
	}
	return
}

func (o *S3SplitFileOutput) cleanDim(dim string) (cleaned string) {
	return cleanPattern.ReplaceAllString(dim, "_")
}

func (o *S3SplitFileOutput) rotateFiles() (err error) {
	var n = time.Now().UTC()
	for dims, fileInfo := range o.dimFiles {
		ageNanos := n.Sub(fileInfo.lastUpdate).Nanoseconds()
		//fmt.Printf("Age: %d, Max: %d\n", ageNanos, int64(o.MaxFileAge) * 1000000)
		if ageNanos > int64(o.MaxFileAge) * 1000000 {
			// Remove old file from dimFiles
			delete(o.dimFiles, dims)

			// Then finalize it
			if e := o.finalizeOne(fileInfo.name); e != nil {
				err = e
			}
		} /* else {
			fmt.Printf("No need to finalize %s\n", fileInfo.name)
		} */
	}
	return
}

func (o *S3SplitFileOutput) finalizeAll() (err error) {
	for _, fileInfo := range o.dimFiles {
		if e := o.finalizeOne(fileInfo.name); e != nil {
			err = e
		}
	}
	return
}

func (o *S3SplitFileOutput) getCurrentFileName(fileName string) (fullPath string) {
	return filepath.Join(o.Path, stdCurrentDir, fileName)
}

func (o *S3SplitFileOutput) getFinalizedFileName(fileName string) (fullPath string) {
	return filepath.Join(o.Path, stdFinalizedDir, fileName)
}

func (o *S3SplitFileOutput) finalizeOne(fileName string) (err error) {
	oldName := o.getCurrentFileName(fileName)
	newName := o.getFinalizedFileName(fileName)
	//fmt.Printf("Moving '%s' to '%s'\n", oldName, newName)

	newPath := filepath.Dir(newName)
	if err = os.MkdirAll(newPath, o.folderPerm); err != nil {
		return fmt.Errorf("S3SplitFileOutput can't create the finalized path %s: %s", newPath, err)
	}

	return os.Rename(oldName, newName)
}

func (o *S3SplitFileOutput) getNewFilename() (name string) {
	// Mon Jan 2 15:04:05 -0700 MST 2006
	return fmt.Sprintf("%s_%s", time.Now().UTC().Format("20060102150405.000"), hostname)
}

func (o *S3SplitFileOutput) getDimPath(pack *PipelinePack) (dimPath string) {
	dims := []string{"UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"}
	remaining := len(dims)
	for _, field := range pack.Message.Fields {
		if remaining == 0 {
			break
		}
		// TODO: schema-powered, not hard-coded.
		idx := -1
		dim := field.GetValue().(string)
		switch field.GetName()  {
		case "submissionDate":
			idx = 0
		case "sourceName":
			idx = 1
		case "sourceVersion":
			idx = 2
		case "appName":
			idx = 3
		case "appUpdateChannel":
			idx = 4
            if !acceptableChannels[dim] {
				dim = "OTHER"
            }
		case "appVersion":
			idx = 5
		}
		if idx >= 0 {
			dims[idx] = dim
			remaining -= 1
		}
    }

    return strings.Join(dims, "/")
}

func (o *S3SplitFileOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	enc := or.Encoder()
	if enc == nil {
		return errors.New("Encoder required.")
	}
	if o.UseFraming == nil {
		// Nothing was specified, we'll default to framing IFF ProtobufEncoder
		// is being used.
		if _, ok := enc.(*ProtobufEncoder); ok {
			or.SetUseFraming(true)
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go o.receiver(or, &wg)
	wg.Wait()
	return
}

// Runs in a separate goroutine, accepting incoming messages
func (o *S3SplitFileOutput) receiver(or OutputRunner, wg *sync.WaitGroup) {
	var (
		pack            *PipelinePack
		e               error
		timer           *time.Timer
		timerDuration   time.Duration
		outBytes        []byte
	)
	ok := true
	inChan := or.InChan()

	timerDuration = time.Duration(o.FlushInterval) * time.Millisecond
	if o.FlushInterval > 0 {
		timer = time.NewTimer(timerDuration)
		if o.timerChan == nil { // Tests might have set this already.
			o.timerChan = timer.C
		}
	}

	// TODO: listen for SIGHUP and finalize all current files.
	//          see file_output.go for an example

	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				// Closed inChan => we're shutting down, finalize data files
				o.finalizeAll()
				break
			}
			dimPath := o.getDimPath(pack)
			fileInfo, ok := o.dimFiles[dimPath]
			if !ok {
				fileInfo = &SplitFileInfo{
					name:       filepath.Join(dimPath, o.getNewFilename()),
					lastUpdate: time.Now().UTC(),
				}
				o.dimFiles[dimPath] = fileInfo
			}

			// Encode the message
			if outBytes, e = or.Encode(pack); e != nil {
				or.LogError(e)
			} else if outBytes != nil {
				// Write to split file
				doRotate, err := o.writeMessage(fileInfo.name, outBytes)

				if err != nil {
					or.LogError(fmt.Errorf("Error writing message to %s: %s", fileInfo.name, err))
				} /*else {
					msgCounter++
				}*/

				if doRotate {
					// Rotate fullFile
					// Remove current file from the map (which will trigger the
					// next record with this path to generate a new one)
					//fmt.Printf("TODO: We should rotate '%s'\n", fileInfo.name)
					delete(o.dimFiles, dimPath)
					if e = o.finalizeOne(fileInfo.name); e != nil {
						or.LogError(fmt.Errorf("Error finalizing %s: %s", fileInfo.name, e))
					}
				}
			} else {
				or.LogError(fmt.Errorf("Zero-byte message... why?"))
			}

			pack.Recycle()

			// Trigger immediately when the message count threshold has been
			// reached if a) the "OR" operator is in effect or b) the
			// flushInterval is 0 or c) the flushInterval has already elapsed.
			// at least once since the last flush.
			// if msgCounter >= o.FlushCount {
			// 	if !o.flushOpAnd || o.FlushInterval == 0 || intervalElapsed {
			// 		// This will block until the other side is ready to accept
			// 		// this batch, freeing us to start on the next one.
			// 		o.batchChan <- outBatch
			// 		outBatch = <-o.backChan
			// 		msgCounter = 0
			// 		intervalElapsed = false
			// 		if timer != nil {
			// 			timer.Reset(timerDuration)
			// 		}
			// 	}
			// }
		case <-o.timerChan:
			//fmt.Printf("TODO: check for rotate by time\n")
			if e = o.rotateFiles(); e != nil {
				or.LogError(fmt.Errorf("Error rotating files by time: %s", e))
			}

			// if (o.flushOpAnd && msgCounter >= o.FlushCount) ||
			// 	(!o.flushOpAnd && msgCounter > 0) {

			// 	// This will block until the other side is ready to accept
			// 	// this batch, freeing us to start on the next one.
			// 	o.batchChan <- outBatch
			// 	outBatch = <-o.backChan
			// 	msgCounter = 0
			// 	intervalElapsed = false
			// } else {
			// 	intervalElapsed = true
			// }
			timer.Reset(timerDuration)
		}
	}
	wg.Done()
}

// Runs in a separate goroutine, waits for buffered data on the committer
// channel, writes it out to the filesystem, and puts the now empty buffer on
// the return channel for reuse.
// func (o *S3SplitFileOutput) committer(or OutputRunner, wg *sync.WaitGroup) {
// 	initBatch := make([]byte, 0, 10000)
// 	o.backChan <- initBatch
// 	var outBatch []byte
// 	var err error

// 	ok := true
// 	hupChan := make(chan interface{})
// 	notify.Start(RELOAD, hupChan)

// 	for ok {
// 		select {
// 		case outBatch, ok = <-o.batchChan:
// 			if !ok {
// 				// Channel is closed => we're shutting down, exit cleanly.
// 				break
// 			}
// 			n, err := o.file.Write(outBatch)
// 			if err != nil {
// 				or.LogError(fmt.Errorf("Can't write to %s: %s", o.Path, err))
// 			} else if n != len(outBatch) {
// 				or.LogError(fmt.Errorf("Truncated output for %s", o.Path))
// 			} else {
// 				o.file.Sync()
// 			}
// 			outBatch = outBatch[:0]
// 			o.backChan <- outBatch
// 		case <-hupChan:
// 			o.file.Close()
// 			if err = o.openFile(); err != nil {
// 				// TODO: Need a way to handle this gracefully, see
// 				// https://github.com/mozilla-services/heka/issues/38
// 				panic(fmt.Sprintf("S3SplitFileOutput unable to reopen file '%s': %s",
// 					o.Path, err))
// 			}
// 		}
// 	}

// 	o.file.Close()
// 	wg.Done()
// }

func init() {
	RegisterPlugin("S3SplitFileOutput", func() interface{} {
		return new(S3SplitFileOutput)
	})
}

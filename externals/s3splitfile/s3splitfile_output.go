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
//	"runtime/pprof"
)

type SplitFileInfo struct {
	name       string
	lastUpdate time.Time
	file       *os.File
	size       uint32
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
	// In-flight files go to <Path>/current/<dimensionPath>
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

func (o *S3SplitFileOutput) writeMessage(fi *SplitFileInfo, msgBytes []byte) (rotate bool, err error) {
	rotate = false
	// fileName := o.getCurrentFileName(fileSuffix)
	// filePath := filepath.Dir(fileName)

	// if e := os.MkdirAll(filePath, o.folderPerm); e != nil {
	// 	return rotate, fmt.Errorf("S3SplitFileOutput can't create file path '%s': %s", filePath, e)
	// }

	// f, e := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, o.perm)
	// if e != nil {
	// 	return rotate, e
	// }
	// defer f.Close()

	n, e := fi.file.Write(msgBytes)
	fi.size += uint32(n)

	if e != nil {
		return rotate, fmt.Errorf("Can't write to %s: %s", fi.name, e)
	} else if n != len(msgBytes) {
		return rotate, fmt.Errorf("Truncated output for %s", fi.name)
	} else {
		//f.Sync()

		// if fi, e := f.Stat(); e != nil {
		// 	return rotate, e
		// } else {
			if fi.size >= o.MaxFileSize {
				rotate = true
			}
		// }
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
			if e := o.finalizeOne(fileInfo); e != nil {
				err = e
			}
		}
	}
	return
}

func (o *S3SplitFileOutput) finalizeAll() (err error) {
	for _, fileInfo := range o.dimFiles {
		if e := o.finalizeOne(fileInfo); e != nil {
			err = e
		}
	}
	return
}

func (o *S3SplitFileOutput) openCurrent(fi *SplitFileInfo) (file *os.File, err error) {
	fullName := o.getCurrentFileName(fi.name)
	fullPath := filepath.Dir(fullName)
	if err = os.MkdirAll(fullPath, o.folderPerm); err != nil {
		return nil, fmt.Errorf("S3SplitFileOutput can't create path %s: %s", fullPath, err)
	}

	file, err = os.OpenFile(fullName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, o.perm)
	return
}

func (o *S3SplitFileOutput) getCurrentFileName(fileName string) (fullPath string) {
	return filepath.Join(o.Path, stdCurrentDir, fileName)
}

func (o *S3SplitFileOutput) getFinalizedFileName(fileName string) (fullPath string) {
	return filepath.Join(o.Path, stdFinalizedDir, fileName)
}

func (o *S3SplitFileOutput) finalizeOne(fi *SplitFileInfo) (err error) {
	fi.file.Close()
	oldName := o.getCurrentFileName(fi.name)
	newName := o.getFinalizedFileName(fi.name)
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

	profile, e := os.Create(filepath.Join(o.Path, "splitfile.prof"))
    if e != nil {
        err = e
        return
    }
    pprof.StartCPUProfile(profile)

	var wg sync.WaitGroup
	wg.Add(1)
	go o.receiver(or, &wg)
	wg.Wait()
	pprof.StopCPUProfile()
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
					size:       0,
				}
				f, e := o.openCurrent(fileInfo)
				if e != nil {
					or.LogError(fmt.Errorf("Error opening file: %s", e))
				}
				fileInfo.file = f
				o.dimFiles[dimPath] = fileInfo
			}

			// Encode the message
			if outBytes, e = or.Encode(pack); e != nil {
				or.LogError(e)
			} else if outBytes != nil {
				// Write to split file
				doRotate, err := o.writeMessage(fileInfo, outBytes)

				if err != nil {
					or.LogError(fmt.Errorf("Error writing message to %s: %s", fileInfo.name, err))
				}

				if doRotate {
					// Remove current file from the map (which will trigger the
					// next record with this path to generate a new one)
					delete(o.dimFiles, dimPath)
					if e = o.finalizeOne(fileInfo); e != nil {
						or.LogError(fmt.Errorf("Error finalizing %s: %s", fileInfo.name, e))
					}
				}
			} else {
				or.LogError(fmt.Errorf("Zero-byte message... why?"))
			}

			pack.Recycle()
		case <-o.timerChan:
			if e = o.rotateFiles(); e != nil {
				or.LogError(fmt.Errorf("Error rotating files by time: %s", e))
			}
			timer.Reset(timerDuration)
		}
	}
	wg.Done()
}

func init() {
	RegisterPlugin("S3SplitFileOutput", func() interface{} {
		return new(S3SplitFileOutput)
	})
}

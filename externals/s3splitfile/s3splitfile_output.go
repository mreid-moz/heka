/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

package s3splitfile

import (
	"errors"
	"fmt"
	. "github.com/mozilla-services/heka/pipeline"
	"github.com/mozilla-services/heka/plugins"
	"github.com/rafrombrc/go-notify"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Output plugin that writes message contents to a file on the file system.
type S3SplitFileOutput struct {
	*S3SplitFileOutputConfig
	perm       os.FileMode
	flushOpAnd bool
	file       *os.File
	batchChan  chan []byte
	backChan   chan []byte
	folderPerm os.FileMode
	timerChan  <-chan time.Time
}

// ConfigStruct for FileOutput plugin.
type S3SplitFileOutputConfig struct {
	// Full output file path.
	Path string

	// Output file permissions (default "644").
	Perm string

	// Interval at which accumulated file data should be written to disk, in
	// milliseconds (default 1000, i.e. 1 second). Set to 0 to disable.
	FlushInterval uint32 `toml:"flush_interval"`

	// Number of messages to accumulate until file data should be written to
	// disk (default 1, minimum 1).
	FlushCount uint32 `toml:"flush_count"`

	// Operator describing how the two parameters "flush_interval" and
	// "flush_count" are combined. Allowed values are "AND" or "OR" (default is
	// "AND").
	FlushOperator string `toml:"flush_operator"`

	// Permissions to apply to directories created for FileOutput's parent
	// directory if it doesn't exist.  Must be a string representation of an
	// octal integer. Defaults to "700".
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

func (o *S3SplitFileOutput) ConfigStruct() interface{} {
	return &S3SplitFileOutputConfig{
		Perm:          "644",
		FlushInterval: 1000,
		FlushCount:    1,
		FlushOperator: "AND",
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
	if err = o.openFile(); err != nil {
		err = fmt.Errorf("S3SplitFileOutput '%s' error opening file: %s", o.Path, err)
		return
	}

	if conf.FlushCount < 1 {
		err = fmt.Errorf("Parameter 'flush_count' must be greater than 0.")
		return
	}
	if conf.MaxFileSize < 1 {
		err = fmt.Errorf("Parameter 'max_file_size' must be greater than 0.")
		return
	}
	if conf.MaxFileAge < 1 {
		err = fmt.Errorf("Parameter 'max_file_age' must be greater than 0.")
		return
	}

	switch conf.FlushOperator {
	case "AND":
		o.flushOpAnd = true
	case "OR":
		o.flushOpAnd = false
	default:
		err = fmt.Errorf("Parameter 'flush_operator' needs to be either 'AND' or 'OR', is currently: '%s'",
			conf.FlushOperator)
		return
	}

	// TODO: wat
	o.batchChan = make(chan []byte)
	o.backChan = make(chan []byte, 2) // Never block on the hand-back
	return
}

func (o *S3SplitFileOutput) openFile() (err error) {
	basePath := filepath.Dir(o.Path)
	if err = os.MkdirAll(basePath, o.folderPerm); err != nil {
		return fmt.Errorf("Can't create the basepath for the S3SplitFileOutput plugin: %s", err.Error())
	}
	if err = plugins.CheckWritePermission(basePath); err != nil {
		return
	}
	o.file, err = os.OpenFile(o.Path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, o.perm)
	return
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
	wg.Add(2)
	go o.receiver(or, &wg)
	go o.committer(or, &wg)
	wg.Wait()
	return
}

// Runs in a separate goroutine, accepting incoming messages, buffering output
// data until the ticker triggers the buffered data should be put onto the
// committer channel.
func (o *S3SplitFileOutput) receiver(or OutputRunner, wg *sync.WaitGroup) {
	var (
		pack            *PipelinePack
		e               error
		timer           *time.Timer
		timerDuration   time.Duration
		msgCounter      uint32
		intervalElapsed bool
		outBytes        []byte
	)
	ok := true
	outBatch := make([]byte, 0, 10000)
	inChan := or.InChan()

	timerDuration = time.Duration(o.FlushInterval) * time.Millisecond
	if o.FlushInterval > 0 {
		timer = time.NewTimer(timerDuration)
		if o.timerChan == nil { // Tests might have set this already.
			o.timerChan = timer.C
		}
	}

	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				// Closed inChan => we're shutting down, flush data
				if len(outBatch) > 0 {
					o.batchChan <- outBatch
				}
				close(o.batchChan)
				break
			}
			if outBytes, e = or.Encode(pack); e != nil {
				or.LogError(e)
			} else if outBytes != nil {
				outBatch = append(outBatch, outBytes...)
				msgCounter++
			}
			pack.Recycle()

			// Trigger immediately when the message count threshold has been
			// reached if a) the "OR" operator is in effect or b) the
			// flushInterval is 0 or c) the flushInterval has already elapsed.
			// at least once since the last flush.
			if msgCounter >= o.FlushCount {
				if !o.flushOpAnd || o.FlushInterval == 0 || intervalElapsed {
					// This will block until the other side is ready to accept
					// this batch, freeing us to start on the next one.
					o.batchChan <- outBatch
					outBatch = <-o.backChan
					msgCounter = 0
					intervalElapsed = false
					if timer != nil {
						timer.Reset(timerDuration)
					}
				}
			}
		case <-o.timerChan:
			if (o.flushOpAnd && msgCounter >= o.FlushCount) ||
				(!o.flushOpAnd && msgCounter > 0) {

				// This will block until the other side is ready to accept
				// this batch, freeing us to start on the next one.
				o.batchChan <- outBatch
				outBatch = <-o.backChan
				msgCounter = 0
				intervalElapsed = false
			} else {
				intervalElapsed = true
			}
			timer.Reset(timerDuration)
		}
	}
	wg.Done()
}

// Runs in a separate goroutine, waits for buffered data on the committer
// channel, writes it out to the filesystem, and puts the now empty buffer on
// the return channel for reuse.
func (o *S3SplitFileOutput) committer(or OutputRunner, wg *sync.WaitGroup) {
	initBatch := make([]byte, 0, 10000)
	o.backChan <- initBatch
	var outBatch []byte
	var err error

	ok := true
	hupChan := make(chan interface{})
	notify.Start(RELOAD, hupChan)

	for ok {
		select {
		case outBatch, ok = <-o.batchChan:
			if !ok {
				// Channel is closed => we're shutting down, exit cleanly.
				break
			}
			n, err := o.file.Write(outBatch)
			if err != nil {
				or.LogError(fmt.Errorf("Can't write to %s: %s", o.Path, err))
			} else if n != len(outBatch) {
				or.LogError(fmt.Errorf("Truncated output for %s", o.Path))
			} else {
				o.file.Sync()
			}
			outBatch = outBatch[:0]
			o.backChan <- outBatch
		case <-hupChan:
			o.file.Close()
			if err = o.openFile(); err != nil {
				// TODO: Need a way to handle this gracefully, see
				// https://github.com/mozilla-services/heka/issues/38
				panic(fmt.Sprintf("S3SplitFileOutput unable to reopen file '%s': %s",
					o.Path, err))
			}
		}
	}

	o.file.Close()
	wg.Done()
}

func init() {
	RegisterPlugin("S3SplitFileOutput", func() interface{} {
		return new(S3SplitFileOutput)
	})
}

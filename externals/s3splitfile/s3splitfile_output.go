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
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"io/ioutil"
	"encoding/json"
)

// Output plugin that writes message contents to a file on the file system.
type S3SplitFileOutput struct {
	*S3SplitFileOutputConfig
	perm       os.FileMode
	folderPerm os.FileMode
	timerChan  <-chan time.Time
	dimFiles   map[string]*SplitFileInfo
	schema     Schema
}

// ConfigStruct for S3SplitFileOutput plugin.
type S3SplitFileOutputConfig struct {
	// Base output file path.
	// In-flight files go to <Path>/current/<dimensionPath>
	// finalized files go to <Path>/finalized/<dimensionPath>
	Path string

	// Output file permissions (default "644").
	Perm string

	// Path to Schema file (json). Defaults to using the standard schema.
	SchemaFile string `toml:"schema_file"`

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

// Info for a single split file
type SplitFileInfo struct {
	name       string
	lastUpdate time.Time
	file       *os.File
	size       uint32
}

// Encapsulates the directory-splitting schema
type Schema struct {
	Fields []string
	FieldIndices map[string]int
	Dims map[string]DimensionChecker
}

// Determine whether a given value is acceptable for a given field, and if not
// return a default value instead.
func (o *Schema) GetValue(field string, value string) (rvalue string, err error) {
	checker, ok := o.Dims[field]
	if ok {
		if checker.IsAllowed(value) {
			return value, nil
		} else {
			return "OTHER", nil
		}
	}
	return value, fmt.Errorf("No such field: '%s'", field)
}

// Extract all dimensions from the given pack.
func (o *Schema) getDimensions(pack *PipelinePack) (dimensions []string) {
	dims := make([]string, len(o.Fields))
	for i, _ := range dims {
		dims[i] = "UNKNOWN"
	}

	// TODO: add support for top-level message fields (Timestamp, etc)
	remaining := len(dims)
	for _, field := range pack.Message.Fields {
		if remaining == 0 {
			break
		}

		idx, ok := o.FieldIndices[field.GetName()]
		if ok {
			remaining -= 1
			v, err := o.GetValue(field.GetName(), field.GetValue().(string))
			if err != nil {
				fmt.Printf("How did this happen? %s", err)
			}
			dims[idx] = v
		}
	}

	return dims
}

// Interface for calculating whether a particular value is acceptable
// as-is, or if it should be replaced with a default value.
type DimensionChecker interface {
	IsAllowed(v string) (bool)
}

// Accept any value at all.
type AnyDimensionChecker struct {
}
func (o AnyDimensionChecker) IsAllowed(v string) (bool) {
	return true
}

// Accept a specific list of values, anything not in the list
// will not be accepted
// TODO: use a map[string]bool for inclusion checking.
type ListDimensionChecker struct {
	// Use a map instead of a list internally for fast lookups.
	allowed map[string]bool
}
func (o ListDimensionChecker) IsAllowed(v string) (bool) {
	_, ok := o.allowed[v]
	return ok
	// for _, a := range o.allowed {
	// 	if a == v {
	// 		return true
	// 	}
	// }
	// return false
}

// Factory for creating a ListDimensionChecker using a list instead of a map
func NewListDimensionChecker(allowed []string) *ListDimensionChecker {
	dimMap := map[string]bool{}
	for _, a := range(allowed) {
		dimMap[a] = true
	}
	return &ListDimensionChecker{dimMap}
}

// If both are specified, accept any value between `min` and `max` (inclusive).
// If one of the bounds is missing, only enforce the other. If neither bound is
// present, accept all values.
type RangeDimensionChecker struct {
	min string
	max string
}
func (o RangeDimensionChecker) IsAllowed(v string) (bool) {
	// Min and max are optional, so treat them separately.
	if o.min != "" && o.min > v {
		return false
	}

	if o.max != "" && o.max < v {
		return false
	}

	return true
}

var hostname, _ = os.Hostname()

// Pattern to use for sanitizing path/file components.
var cleanPattern = regexp.MustCompile("[^a-zA-Z0-9_/.]")

// Names for the subdirectories to use for in-flight and finalized files. These
// dirs are found under the main Path specified in the config.
const (
	stdCurrentDir   = "current"
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

func (o *S3SplitFileOutput) loadSchema(schemaFileName string) (schema Schema, err error) {
	// Placeholder for parsing JSON
	type JSchemaDimension struct {
		Field_name string
		Allowed_values interface{}
	}

	// Placeholder for parsing JSON
	type JSchema struct {
		Version int32
		Dimensions []JSchemaDimension
	}

	schemaBytes, err := ioutil.ReadFile(schemaFileName)
	if err != nil {
		return
	}

	var js JSchema

	err = json.Unmarshal(schemaBytes, &js)
	if err != nil {
		return
	}

	fields := make([]string, len(js.Dimensions))
	fieldIndices := map[string]int{}
	dims := map[string]DimensionChecker{}
	schema = Schema{fields, fieldIndices, dims}

	for i, d := range js.Dimensions {
		schema.Fields[i] = d.Field_name
		schema.FieldIndices[d.Field_name] = i
		switch d.Allowed_values.(type) {
		case string:
			if d.Allowed_values.(string) == "*" {
				schema.Dims[d.Field_name] = AnyDimensionChecker{}
			} else {
				schema.Dims[d.Field_name] = NewListDimensionChecker([]string{d.Allowed_values.(string)})
			}
		case []interface{}:
			allowed := make([]string, len(d.Allowed_values.([]interface{})))
			for i, v := range d.Allowed_values.([]interface{}) {
				allowed[i] = v.(string)
			}
			schema.Dims[d.Field_name] = NewListDimensionChecker(allowed)
		case map[string]interface{}:
			vrange := d.Allowed_values.(map[string]interface{})
			schema.Dims[d.Field_name] = RangeDimensionChecker{vrange["min"].(string), vrange["max"].(string)}
		}
	}
	return
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

	// TODO: fall back to default schema.
	//fmt.Printf("schema_file = '%s'\n", conf.SchemaFile)
	if conf.SchemaFile == "" {
		err = fmt.Errorf("Parameter 'schema_file' is missing")
		return
	}

	o.schema, err = o.loadSchema(conf.SchemaFile)
	if err != nil {
		err = fmt.Errorf("Parameter 'schema_file' must be a valid JSON file: %s", err)
	}

	return
}

func (o *S3SplitFileOutput) writeMessage(fi *SplitFileInfo, msgBytes []byte) (rotate bool, err error) {
	rotate = false
	n, e := fi.file.Write(msgBytes)

	// Note that if these files are being written to elsewhere, the size-based
	// rotation will not work as expected. A more robust approach would be to
	// use something like `fi.file.Seek(0, os.SEEK_CUR)` to get the current
	// offset into the file.
	fi.size += uint32(n)

	if e != nil {
		return rotate, fmt.Errorf("Can't write to %s: %s", fi.name, e)
	} else if n != len(msgBytes) {
		return rotate, fmt.Errorf("Truncated output for %s", fi.name)
	} else {
		if fi.size >= o.MaxFileSize {
			rotate = true
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
		if ageNanos > int64(o.MaxFileAge)*1000000 {
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
	dims := o.schema.getDimensions(pack)
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
		pack          *PipelinePack
		e             error
		timer         *time.Timer
		timerDuration time.Duration
		outBytes      []byte
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
			// fmt.Printf("Found a path: %s\n", dimPath)
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

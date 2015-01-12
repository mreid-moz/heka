/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mark Reid (mark@mozilla.com)
# ***** END LICENSE BLOCK *****/

/*

A command-line utility for exporting heka output files to Amazon S3.

*/
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
)

type AWSCredentials struct {
	Key string
	SecretKey string
	Region string
}

type Progress struct {
	Count int64
	Bytes int64
	Errors int32
}

func makeupload(base string, pattern *regexp.Regexp, creds AWSCredentials, bucket string, bucketPrefix string, dryRun bool, progress *Progress) func(string, os.FileInfo, error) error {
	//s3Prefix := fmt.Sprintf("s3://%s", bucket)
	//if bucketPrefix != "" {
	//	s3Prefix = fmt.Sprintf("%s/%s", s3Prefix, strings.Trim(bucketPrefix, "/"))
	//}
	bucketPrefix = strings.Trim(bucketPrefix, "/")

	cleanBase := filepath.Clean(base)

	var s *s3.S3
	var b *s3.Bucket
	if !dryRun {
		auth := aws.Auth{AccessKey: creds.Key, SecretKey: creds.SecretKey}
		s = s3.New(auth, aws.Regions[creds.Region])
		b = s.Bucket(bucket)
	} else {
		// s declared and not used :(
		_ = s
		_ = b
	}

	return func(path string, fi os.FileInfo, err error) (errOut error) {
		if err != nil {
			return err
		}

		if fi.IsDir() {
			return nil
		}
		//fmt.Printf("Found an item: %s\n", path)

		baseName := filepath.Base(path)
		if !pattern.MatchString(baseName) {
			//fmt.Printf("Item does not match pattern\n")
			return nil
		}

		// Make sure we're comparing apples to apples when stripping off the
		// base path.
		cleanPath := filepath.Clean(path)
		s3Path := fmt.Sprintf("%s/%s", bucketPrefix, cleanPath[len(cleanBase)+1:])

		if dryRun {
			fmt.Printf("Dry run. Not uploading item to %s\n", s3Path)
		} else {
			fmt.Printf("Uploading item to: %s\n", s3Path)
			reader, err := os.Open(path)
			if err != nil {
				fmt.Printf("Error opening %s for reading: %s\n", path, err)
				errOut = err
				progress.Errors++
			} else {
				err := b.PutReader(s3Path, reader, fi.Size(), "binary/octet-stream", s3.BucketOwnerFull, s3.Options{})
				if err != nil {
					errOut = err
					progress.Errors++
				} else {
					err := os.Remove(path)
					if err != nil {
						fmt.Printf("Error removing %s: %s\n", path, err)
						errOut = err
						progress.Errors++
					}
				}
			}

		}

		progress.Count += 1
		progress.Bytes += fi.Size()

		return errOut
	}
}


func main() {
    flagBase := flag.String("base-dir", "/", "Base directory in which to look for files to export")
    flagPattern := flag.String("pattern", ".*", "Filenames must match this regular expression to be uploaded")
    flagBucket := flag.String("bucket", "default-bucket", "S3 Bucket name")
    flagBucketPrefix := flag.String("bucket-prefix", "", "S3 Bucket path prefix")
    flagAWSKey := flag.String("aws-key", "DUMMY", "AWS Key")
    flagAWSSecretKey := flag.String("aws-secret-key", "DUMMY", "AWS Secret Key")
    flagAWSRegion := flag.String("aws-region", "us-west-2", "AWS Region")
    flagLoop := flag.Bool("loop", false, "Run in a loop and keep watching for more files to export")
    flagDryRun := flag.Bool("dry-run", false, "Don't actually do anything, just output what would be done")
	flag.Parse()

	if flag.NArg() != 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	var err error
	baseStat, err := os.Stat(*flagBase)
	if err != nil || !baseStat.IsDir() {
		fmt.Printf("base-dir: %s\n", err)
		os.Exit(2)
	}

	pattern, err := regexp.Compile(*flagPattern)
	if err != nil {
		fmt.Printf("pattern: %s\n", err)
		os.Exit(3)
	}

	fmt.Printf("Base:%s  Pattern:%s  Bucket: s3://%s/%s  AWSKey:%s / %s  Region:%s  Dry Run:%t  Loop:%t\n",
		*flagBase, *flagPattern, *flagBucket, *flagBucketPrefix, *flagAWSKey, *flagAWSSecretKey, *flagAWSRegion, *flagDryRun, *flagLoop)

	var progress Progress
	var rate float64
	var uploadMB float64

	startTime := time.Now().UTC()

	credentials := AWSCredentials{*flagAWSKey, *flagAWSSecretKey, *flagAWSRegion}
	err = filepath.Walk(*flagBase, makeupload(*flagBase, pattern, credentials, *flagBucket, *flagBucketPrefix, *flagDryRun, &progress))
	if err != nil {
		fmt.Printf("Error reading files from %s: %s\n", *flagBase, err)
		os.Exit(4)
	}

	uploadMB = float64(progress.Bytes) / 1024.0 / 1024.0
	duration := time.Now().UTC().Sub(startTime).Seconds()

	if duration > 0 {
		rate = uploadMB / duration
	}
	fmt.Printf("Uploaded %d files containing %.2fMB in %.02fs (%.02fMB/s). Encountered %d errors.\n", progress.Count, uploadMB, duration, rate, progress.Errors)
}

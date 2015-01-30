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

A command-line utility for listing files on Amazon S3, filtered by dimension.

*/
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strings"
	"time"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/mreid-moz/s3splitfile"
)

var suffixes = [...]string{"", "K", "M", "G", "T", "P"}

func PrettySize(bytes int64) (string) {
	fBytes := float64(bytes)
	sIdx := 0
	for i, _ := range suffixes {
		sIdx = i
		if fBytes < math.Pow(1024.0, float64(sIdx + 1)) {
			break
		}
	}

	pretty := fBytes / math.Pow(1024.0, float64(sIdx))
	return fmt.Sprintf("%.2f%sB", pretty, suffixes[sIdx])
}

func main() {
    flagSchema := flag.String("schema", "", "Filename of the schema to use as a filter")
    flagBucket := flag.String("bucket", "default-bucket", "S3 Bucket name")
    flagBucketPrefix := flag.String("bucket-prefix", "", "S3 Bucket path prefix")
    flagAWSKey := flag.String("aws-key", "DUMMY", "AWS Key")
    flagAWSSecretKey := flag.String("aws-secret-key", "DUMMY", "AWS Secret Key")
    flagAWSRegion := flag.String("aws-region", "us-west-2", "AWS Region")
    flagDryRun := flag.Bool("dry-run", false, "Don't actually do anything, just output what would be done")
    flagVerbose := flag.Bool("verbose", false, "Print detailed info")
	flag.Parse()

	if flag.NArg() != 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	var err error
	var schema s3splitfile.Schema
	schema, err = s3splitfile.LoadSchema(*flagSchema)
	if err != nil {
		fmt.Printf("schema: %s\n", err)
		os.Exit(2)
	}

	if *flagDryRun {
		fmt.Printf("Dry Run: Would have listed files in s3://%s/%s according to filter schema %s\n",
				   *flagBucket, *flagBucketPrefix, *flagSchema)
		os.Exit(0)
	}

	var b *s3.Bucket

	prefix := strings.Trim(*flagBucketPrefix, "/")
	if prefix != "" {
		prefix += "/"
	}

	// prefix := strings.Trim(*flagBucketPrefix, "/") + "/"

	// Initialize the S3 bucket
	auth := aws.Auth{AccessKey: *flagAWSKey, SecretKey: *flagAWSSecretKey}
	s := s3.New(auth, aws.Regions[*flagAWSRegion])
	b = s.Bucket(*flagBucket)

	var errCount int
	var totalCount int
	var totalSize int64

	startTime := time.Now().UTC()

	// List the keys as we see them
	for k := range s3splitfile.S3Iterator(b, prefix, schema) {
		if k.Err != nil {
			fmt.Printf("ERROR fetching key: %s\n", k.Err)
			errCount++
		} else {
			totalCount++
			totalSize += k.Key.Size
			fmt.Printf("%s\n", k.Key.Key)
		}
	}

	duration := time.Now().UTC().Sub(startTime).Seconds()

	if *flagVerbose {
		fmt.Printf("Filter matched %d files totaling %s in %.02fs (%d errors)\n", totalCount, PrettySize(totalSize), duration, errCount)
	}
}

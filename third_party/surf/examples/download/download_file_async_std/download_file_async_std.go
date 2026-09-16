package main

import (
	"bytes"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/enetx/g"
	"github.com/enetx/surf"
)

func main() {
	const dURL = "https://jsoncompare.org/LearningContainer/SampleFiles/Video/MP4/Sample-Video-File-For-Testing.mp4"

	r := surf.NewClient().Head(dURL).Do()
	if r.IsErr() {
		log.Fatal(r.Err())
	}

	if r.Ok().Headers.Get("Accept-Ranges") != "bytes" {
		log.Fatal("Doesn't support header 'Accept-Ranges'.")
	}

	fileSize, err := strconv.Atoi(r.Ok().Headers.Get("Content-Length").Std())
	if err != nil {
		log.Fatal(err)
	}

	var (
		workers   = 10
		chunkSize = fileSize / workers
		diff      = fileSize % workers
		tmpFiles  []string
		wg        sync.WaitGroup
		mu        sync.Mutex
		errOnce   sync.Once
		writeErr  error
	)

	setWriteErr := func(err error) {
		if err != nil {
			errOnce.Do(func() { writeErr = err })
		}
	}

	for i := range workers {
		wg.Add(1)

		min := chunkSize * i
		max := chunkSize * (i + 1)

		if i == workers-1 {
			max += diff
		}

		go func(min, max, i int) {
			defer wg.Done()

			headers := map[string]string{
				"Range": "bytes=" + strconv.Itoa(min) + "-" + strconv.Itoa(max-1),
			}

			r := surf.NewClient().
				Builder().
				Retry(10, time.Second*2).
				AddHeaders(headers).
				Build().
				Unwrap().
				Get(dURL).
				Do()

			if r.IsErr() {
				setWriteErr(r.Err())
				return
			}

			tmpFile, err := os.CreateTemp("", strconv.Itoa(i)+".")
			if err != nil {
				setWriteErr(err)
				return
			}

			err = r.Ok().Body.Dump(g.String(tmpFile.Name()))
			if err != nil {
				setWriteErr(err)
				return
			}

			mu.Lock()
			tmpFiles = append(tmpFiles, tmpFile.Name())
			mu.Unlock()
		}(min, max, i)
	}

	wg.Wait()

	if writeErr != nil {
		removeFiles(tmpFiles)
		log.Fatal(writeErr)
	}

	sortFiles(tmpFiles)

	out, err := mergeFiles(tmpFiles)
	if err != nil {
		log.Fatal(err)
	}

	if out.Len() != fileSize {
		log.Fatal("file not downloading properly")
	}

	pURL, err := url.ParseRequestURI(dURL)
	if err != nil {
		log.Fatal(err)
	}

	os.WriteFile(path.Base(pURL.Path), out.Bytes(), 0o644)
}

func sortFiles(files []string) {
	sort.Slice(files, func(i, j int) bool {
		a, _, _ := strings.Cut(filepath.Base(files[i]), ".")
		ai, _ := strconv.Atoi(a)

		b, _, _ := strings.Cut(filepath.Base(files[j]), ".")
		bi, _ := strconv.Atoi(b)
		return ai < bi
	})
}

func mergeFiles(files []string) (bytes.Buffer, error) {
	defer removeFiles(files)

	var out bytes.Buffer

	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			return out, err
		}

		_, err = out.Write(content)
		if err != nil {
			return out, err
		}
	}

	return out, nil
}

func removeFiles(files []string) {
	for _, file := range files {
		os.Remove(file)
	}
}

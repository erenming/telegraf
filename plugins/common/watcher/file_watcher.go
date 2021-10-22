package watcher

import (
	"fmt"
	"log"
	"strings"

	"github.com/fsnotify/fsnotify"
)

const (
	clusterCredentialPath   = "/erda-cluster-credential"
	clusterCredentialKey    = "CLUSTER_ACCESS_KEY"
	newCredentialCreateFlag = "data"
)

var (
	ClusterCredentialFullPath = fmt.Sprintf("%s/%s", clusterCredentialPath, clusterCredentialKey)
)

type FileWatcher struct {
	filePath string
	watcher  *fsnotify.Watcher
	onEvent  func(event fsnotify.Event)
}

func NewFileWatcher(onEvent func(event fsnotify.Event)) (*FileWatcher, error) {
	var err error

	f := &FileWatcher{
		filePath: clusterCredentialPath,
		onEvent:  onEvent,
	}
	f.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	if err = f.watch(); err != nil {
		return nil, err
	}

	return f, nil
}

func (f *FileWatcher) Close() error {
	return f.watcher.Close()
}

func (f *FileWatcher) watch() error {
	log.Printf("I! [file_watcher] starting watch credential, path: %s", clusterCredentialPath)
	go func(filePath string) {
		for {
			select {
			case event, ok := <-f.watcher.Events:
				if !ok {
					return
				}

				log.Printf("I! [file_watcher] credential file event: %v", event)

				switch event.Op {
				// kubernetes secret mount content change will invoke: CREATE->CHMOD->RENAME->CREATE->REMOVE:
				// watch event CREATE which last time and path contain `data`, first CREATE event will be backup old credential.
				case fsnotify.Create:
					// filter
					if strings.Contains(event.Name, newCredentialCreateFlag) {
						f.onEvent(event)
					}
				default:
					continue
				}

			case err, ok := <-f.watcher.Errors:
				if !ok {
					return
				}
				log.Printf("E! [file_watcher] file watcher error: %v", err)
				return
			}
		}
	}(f.filePath)

	return f.watcher.Add(f.filePath)
}

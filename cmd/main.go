package main

import (
	"os"

	"github.com/cyverse/overlayfs-syncher/syncher"
	log "github.com/sirupsen/logrus"
)

func main() {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "main",
	})

	logger.Logger.SetLevel(log.DebugLevel)

	if len(os.Args) != 3 {
		logger.Fatalf("need two arguments, lower and upper paths")
		os.Exit(1)
	}

	syncher, err := syncher.NewOverlayFSSyncher(os.Args[1], os.Args[2])
	if err != nil {
		logger.Fatalf("%+v", err)
		os.Exit(1)
	}

	//syncher.SetDryRun(true)

	err = syncher.Sync()
	if err != nil {
		logger.Fatalf("%+v", err)
		os.Exit(1)
	}
}

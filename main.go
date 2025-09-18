package dbdt

import "os"

var activeFolder = "."

func SetActiveFolder(folder string) {
	os.Mkdir(folder, os.ModeDir)

	activeFolder = folder
}

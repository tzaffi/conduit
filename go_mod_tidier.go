package main

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"reflect"

	"golang.org/x/mod/modfile"
)

const maxTries = 10
const goMod = "go.mod"

func main() {
	nextModFile, nextSum, err := getAllWithTidy()
	if err != nil {
		panic(fmt.Sprintf("problem with go_mod_tidier round 0: %v", err))
	}
	var prevModFile *modfile.File
	var prevSum string
	var tryNum int
	for tryNum = 1; tryNum <= maxTries; tryNum++ {
		fmt.Printf("go_mod_tidier round %d\n", tryNum)
		prevModFile = nextModFile
		prevSum = nextSum
		var err error
		nextModFile, nextSum, err = getAllWithTidy()
		if err != nil {
			panic(fmt.Sprintf("problem with go_mod_tidier round %d: %v", tryNum, err))
		}
		if reflect.DeepEqual(prevModFile, nextModFile) && prevSum == nextSum {
			break
		}
	}
	if tryNum > maxTries {
		panic(fmt.Sprintf("go_mod_tidier tried %d times and still couldn't get a consistent result", maxTries))
	}
	fmt.Printf("Success! go_mod_tidier took %d tries to obtain a consistent result\n", tryNum-1)
}

func getAllWithTidy() (*modfile.File, string, error) {
	goModBytes, err := ioutil.ReadFile(goMod)
	if err != nil {
		return nil, "", fmt.Errorf("problem reading %s: %w", goMod, err)
	}

	goSumBytes, err := ioutil.ReadFile("go.sum")
	if err != nil {
		return nil, "", fmt.Errorf("problem reading go.sum: %w", err)
	}
	sum := string(goSumBytes)

	mod, err := modfile.Parse(goMod, goModBytes, nil)
	if err != nil {
		return mod, sum, fmt.Errorf("problem parsing %s: %w", goMod, err)
	}

	err = goModTidy(mod)
	if err != nil {
		return mod, sum, err
	}

	direct := make([]string, 0)
	for _, require := range mod.Require {
		if !require.Indirect {
			direct = append(direct, fmt.Sprintf("%s@%s", require.Mod.Path, require.Mod.Version))
		}
	}

	for _, pinnedPkg := range direct {
		fmt.Println("go get'ing", pinnedPkg)
		cmd := exec.Command("go", "get", pinnedPkg)
		err := cmd.Run()
		if err != nil {
			return mod, sum, fmt.Errorf("problem with: go get %s: %w", pinnedPkg, err)
		}
		err = goModTidy(mod)
		if err != nil {
			return mod, sum, err
		}
	}

	return mod, sum, nil
}

func goModTidy(mod *modfile.File) error {
	compatVersion := fmt.Sprintf("-compat=%s", mod.Go.Version)
	fmt.Printf("go mod tidy'ing with %s\n", compatVersion)
	cmdA := exec.Command("go", "mod", "tidy", compatVersion)
	err := cmdA.Run()
	if err != nil {
		return fmt.Errorf("problem with: go mod tidy: %w", err)
	}
	return nil
}

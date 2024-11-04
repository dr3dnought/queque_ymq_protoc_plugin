package main

import (
	"errors"
	"flag"

	"github.com/dr3dnought/queque_ymq_protoc_plugin/generator"
	"google.golang.org/protobuf/compiler/protogen"
)

var (
	ErrMessageNotDefined = errors.New(`message for generation not found`)
)

const messageSuffix = "QueQueMessage"

func main() {
	var flags flag.FlagSet
	msgName := flags.String("msg", "", "message name")

	protogen.Options{
    ParamFunc: flags.Set,
  }.Run(func(gen *protogen.Plugin) error {
		for _, f := range gen.Files {
			if !f.Generate {
				continue
			}
			if msgName == nil || *msgName == "" {
				return ErrMessageNotDefined
			}
			err := generateFile(gen, f, *msgName)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func generateFile(gen *protogen.Plugin, file *protogen.File, msgName string) error {
	filename := file.GeneratedFilenamePrefix + ".quequeymq.pb.go"

	g := gen.NewGeneratedFile(filename, file.GoImportPath)

	generator := generator.New(g, file.GoPackageName, msgName)
	return generator.Generate()
}

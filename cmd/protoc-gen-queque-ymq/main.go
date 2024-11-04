package main

import (
	"errors"
	"flag"
	"slices"
	"strings"

	"github.com/dr3dnought/queque_ymq_protoc_plugin/generator"
	"google.golang.org/protobuf/compiler/protogen"
)

var (
	ErrMessageNotDefined = errors.New(`message for generation not found`)
)

func main() {
	var flags flag.FlagSet
	msgName := flags.String("msg", "", "message name")

	protogen.Options{
		ParamFunc: flags.Set,
	}.Run(func(gen *protogen.Plugin) error {
		// return errors.New(fmt.Sprintf("msg %v", *msgName))
		if msgName == nil || *msgName == "" {
			return ErrMessageNotDefined
		}
		msgs := strings.Split(*msgName, "+")
		for _, f := range gen.Files {
			if !f.Generate {
				continue
			}
			for _, msg := range f.Messages {
				goName := msg.GoIdent.GoName
				if slices.Contains(msgs, goName) {
					err := generateFile(gen, f, goName)
					if err != nil {
						return err
					}

				}
			}
			// if slices.Contains(msgs, )
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

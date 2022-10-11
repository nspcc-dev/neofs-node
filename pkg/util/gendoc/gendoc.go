package gendoc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
	"github.com/spf13/pflag"
)

const (
	gendocTypeFlag = "type"

	gendocMarkdown = "md"
	gendocMan      = "man"

	depthFlag     = "depth"
	extensionFlag = "extension"
)

// Command returns command which generates user documentation for the argument.
func Command(rootCmd *cobra.Command) *cobra.Command {
	gendocCmd := &cobra.Command{
		Use:   "gendoc <dir>",
		Short: "Generate documentation for this command",
		Long: `Generate documentation for this command. If the template is not provided,
builtin cobra generator is used and each subcommand is placed in
a separate file in the same directory.

The last optional argument specifies the template to use with text/template.
In this case there is a number of helper functions which can be used:
  replace STR FROM TO -- same as strings.ReplaceAll
  join ARRAY SEPARATOR -- same as strings.Join
  split STR SEPARATOR -- same as strings.Split
  fullUse CMD -- slice of all command names starting from the parent
  listFlags CMD -- list of command flags
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				_ = cmd.Usage()
				os.Exit(1)
			}

			err := os.MkdirAll(args[0], os.ModePerm)
			if err != nil {
				return fmt.Errorf("can't create directory: %w", err)
			}

			if len(args) == 2 {
				data, err := os.ReadFile(args[1])
				if err != nil {
					return fmt.Errorf("can't read the template '%s': %w", args[1], err)
				}

				return generateTemplate(cmd, rootCmd, args[0], data)
			}

			typ, _ := cmd.Flags().GetString(gendocTypeFlag)
			switch typ {
			case gendocMarkdown:
				return doc.GenMarkdownTree(rootCmd, args[0])
			case gendocMan:
				hdr := &doc.GenManHeader{
					Section: "1",
					Source:  "NSPCC & Morphbits",
				}
				return doc.GenManTree(rootCmd, hdr, args[0])
			default:
				return errors.New("type must be 'md' or 'man'")
			}
		},
	}

	ff := gendocCmd.Flags()
	ff.StringP(gendocTypeFlag, "t", gendocMarkdown, "Type for the documentation ('md' or 'man')")
	ff.Int(depthFlag, 1, "If template is specified, unify all commands starting from depth in a single file. Default: 1.")
	ff.StringP(extensionFlag, "e", "", "If the template is specified, string to append to the output file names")
	return gendocCmd
}

func generateTemplate(cmd *cobra.Command, rootCmd *cobra.Command, outDir string, tmpl []byte) error {
	depth, _ := cmd.Flags().GetInt(depthFlag)
	ext, _ := cmd.Flags().GetString(extensionFlag)

	tm := template.New("doc")
	tm.Funcs(template.FuncMap{
		"replace":   strings.ReplaceAll,
		"split":     strings.Split,
		"join":      strings.Join,
		"fullUse":   fullUse,
		"listFlags": listFlags,
	})

	tm, err := tm.Parse(string(tmpl))
	if err != nil {
		return err
	}

	return visit(rootCmd, outDir, ext, depth, tm)
}

func visit(rootCmd *cobra.Command, outDir string, ext string, depth int, tm *template.Template) error {
	if depth == 0 {
		name := strings.Join(fullUse(rootCmd), "-")
		name = strings.TrimSpace(name)
		name = strings.ReplaceAll(name, " ", "-")
		name = filepath.Join(outDir, name) + ext
		f, err := os.Create(name)
		if err != nil {
			return fmt.Errorf("can't create file '%s': %w", name, err)
		}
		defer f.Close()
		return tm.Execute(f, rootCmd)
	}

	for _, c := range rootCmd.Commands() {
		err := visit(c, outDir, ext, depth-1, tm)
		if err != nil {
			return err
		}
	}
	return nil
}

func fullUse(c *cobra.Command) []string {
	if c == nil {
		return nil
	}
	return append(fullUse(c.Parent()), c.Name())
}

func listFlags(c *cobra.Command) []*pflag.Flag {
	var res []*pflag.Flag
	c.Flags().VisitAll(func(f *pflag.Flag) {
		res = append(res, f)
	})
	return res
}

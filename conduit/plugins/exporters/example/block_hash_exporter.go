package example

import "github.com/algorand/conduit/conduit/plugins/exporters"

const pluginName = "binary-block-hash-exporter"
const pluginPath = "/Users/zeph/github/tzaffi/conduit/conduit/plugins/exporters/example/block_hash_exporter.sh"

func init() {
	exporters.Register(
		pluginName,
		exporters.ExporterConstructorFunc(func() exporters.Exporter {
			return exporters.MakeBinaryExporter(pluginPath)
		}),
	)
}

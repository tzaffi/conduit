package http

// HTTPExporterConfig specific to the http exporter
type HTTPExporterConfig struct {
	// DO I NEED THIS?
	// <code>round</code> is an optional round number for starting the export
	// Round uint64 `yaml:"round"`

	// <code>host</code> is the host to send the POST receive request to
	Port uint16 `yaml:"port"`

	// <code>endpoint</code> is the endpoint to send the POST receive request to
	Endpoint string `yaml:"endpoint"`

	// TODO: currently this only works on unauthenticated local host. Consider supporting
	// external authenticated endpoints as well.
}

package filewriter

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/algorand/go-algorand-sdk/v2/encoding/json"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	"github.com/algorand/go-codec/codec"
)

var prettyHandle *codec.JsonHandle
var jsonStrictHandle *codec.JsonHandle

func init() {
	prettyHandle = new(codec.JsonHandle)
	prettyHandle.ErrorIfNoField = json.CodecHandle.ErrorIfNoField
	prettyHandle.ErrorIfNoArrayExpand = json.CodecHandle.ErrorIfNoArrayExpand
	prettyHandle.Canonical = json.CodecHandle.Canonical
	prettyHandle.RecursiveEmptyCheck = json.CodecHandle.RecursiveEmptyCheck
	prettyHandle.Indent = json.CodecHandle.Indent
	prettyHandle.HTMLCharsAsIs = json.CodecHandle.HTMLCharsAsIs
	prettyHandle.MapKeyAsString = true
	prettyHandle.Indent = 2

	jsonStrictHandle = new(codec.JsonHandle)
	jsonStrictHandle.ErrorIfNoField = prettyHandle.ErrorIfNoField
	jsonStrictHandle.ErrorIfNoArrayExpand = prettyHandle.ErrorIfNoArrayExpand
	jsonStrictHandle.Canonical = prettyHandle.Canonical
	jsonStrictHandle.RecursiveEmptyCheck = prettyHandle.RecursiveEmptyCheck
	jsonStrictHandle.Indent = prettyHandle.Indent
	jsonStrictHandle.HTMLCharsAsIs = prettyHandle.HTMLCharsAsIs
	jsonStrictHandle.MapKeyAsString = true
}

func ParseFilenamePattern(pattern string) (bool, EncodingFormat, error) {
	originalPattern := pattern
	gzip := false
	if strings.HasSuffix(pattern, ".gz") {
		gzip = true
		pattern = pattern[:len(pattern)-3]
	}

	var blockFormat EncodingFormat
	if strings.HasSuffix(pattern, ".msgp") {
		blockFormat = MessagepackFormat
	} else if strings.HasSuffix(pattern, ".json") {
		blockFormat = JSONFormat
	} else {
		return false, UnrecognizedFormat, fmt.Errorf("unrecognized export format: %s", originalPattern)
	}

	return gzip, blockFormat, nil
}

func GenesisFilename(format EncodingFormat, isGzip bool) (string, error) {
	var ext string
	
	switch format {
	case JSONFormat:
		ext = ".json"
	case MessagepackFormat:
		ext = ".msgp"
	default:
		return "", fmt.Errorf("GenesisFilename(): unhandled format %d", format)
	}

	if isGzip {
		ext += ".gz"
	}

	return fmt.Sprintf("genesis%s", ext), nil
}

// EncodeToFile enocods an object to a file using a given format and possible gzip compression.
func EncodeToFile(filename string, v interface{}, format EncodingFormat, isGzip bool) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("EncodeToFile(): failed to create %s: %w", filename, err)
	}
	defer file.Close()

	var writer io.Writer
	if isGzip {
		gz := gzip.NewWriter(file)
		gz.Name = filename
		defer gz.Close()
		writer = gz
	} else {
		writer = file
	}

	return Encode(format, writer, v)
}

func Encode(format EncodingFormat, writer io.Writer, v interface{}) error {
	var encoder *codec.Encoder
	switch format {
	case JSONFormat:
		encoder = codec.NewEncoder(writer, jsonStrictHandle)
	case MessagepackFormat:
		encoder = codec.NewEncoder(writer, msgpack.LenientCodecHandle)
	default:
		return fmt.Errorf("EncodeToFile(): unhandled format %d", format)
	}

	return encoder.Encode(v)
}

// DecodeFromFile decodes a file to an object using a given format and possible gzip compression.
func DecodeFromFile(filename string, v interface{}, format EncodingFormat, isGzip bool) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("DecodeFromFile(): failed to open %s: %w", filename, err)
	}
	defer file.Close()

	var reader io.Reader
	if isGzip {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("DecodeFromFile(): failed to make gzip reader: %w", err)
		}
		defer gz.Close()
		reader = gz
	} else {
		reader = file
	}

	var decoder *codec.Decoder
	switch format {
	case JSONFormat:
		decoder = codec.NewDecoder(reader, jsonStrictHandle)
	case MessagepackFormat:
		decoder = codec.NewDecoder(reader, msgpack.LenientCodecHandle)
	default:
		return fmt.Errorf("DecodeFromFile(): unhandled format %d", format)
	}

	return decoder.Decode(v)
}


// EncodeJSONToFile is used to encode an object to a file. If the file ends in .gz it will be gzipped.
func EncodeJSONToFile(filename string, v interface{}, pretty bool) error {
	var writer io.Writer

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("EncodeJSONToFile(): failed to create %s: %w", filename, err)
	}
	defer file.Close()

	if strings.HasSuffix(filename, ".gz") {
		gz := gzip.NewWriter(file)
		gz.Name = filename
		defer gz.Close()
		writer = gz
	} else {
		writer = file
	}

	var handle *codec.JsonHandle
	if pretty {
		handle = prettyHandle
	} else {
		handle = jsonStrictHandle
	}
	enc := codec.NewEncoder(writer, handle)
	return enc.Encode(v)
}

// DecodeJSONFromFile is used to decode a file to an object.
func DecodeJSONFromFile(filename string, v interface{}, strict bool) error {
	// Streaming into the decoder was slow.
	fileBytes, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("DecodeJSONFromFile(): failed to read %s: %w", filename, err)
	}

	var reader io.Reader = bytes.NewReader(fileBytes)

	if strings.HasSuffix(filename, ".gz") {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("DecodeJSONFromFile(): failed to make gzip reader: %w", err)
		}
		defer gz.Close()
		reader = gz
	}
	var handle *codec.JsonHandle
	if strict {
		handle = json.CodecHandle
	} else {
		handle = json.LenientCodecHandle
	}

	enc := codec.NewDecoder(reader, handle)
	return enc.Decode(v)
}

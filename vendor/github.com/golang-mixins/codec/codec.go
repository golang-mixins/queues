// Package codec presents interface (and its implementation sets) of a codec functions.
package codec

// Marshaler provides marshalling an interface value into byte slice.
type Marshaler interface {
	// Marshal - marshalling an interface value into byte slice.
	Marshal(v interface{}) ([]byte, error)
	// MarshalIndent - marshalling an interface value into byte slice with indent.
	MarshalIndent(v interface{}, prefix, indent string) ([]byte, error)
}

// Unmarshaler provides unmarshalling an byte slice into interface value.
type Unmarshaler interface {
	// Unmarshal - unmarshalling an byte slice into interface value.
	Unmarshal(data []byte, v interface{}) error
	// UnmarshalWithDisallowUnknownFields - unmarshalling an byte slice into interface value with disallow unknown fields.
	UnmarshalWithDisallowUnknownFields(data []byte, v interface{}) error
}

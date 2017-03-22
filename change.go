package osm

import (
	"encoding/xml"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/paulmach/go.osm/internal/osmpb"
)

// Change is the structure of a changeset to be
// uploaded or downloaded from the server.
// See: http://wiki.openstreetmap.org/wiki/OsmChange
type Change struct {
	Version   float64 `xml:"version,attr,omitempty"`
	Generator string  `xml:"generator,attr,omitempty"`

	// Maybe the returned to indicate the origin of the data.
	Copyright   string `xml:"copyright,attr,omitempty"`
	Attribution string `xml:"attribution,attr,omitempty"`
	License     string `xml:"license,attr,omitempty"`

	Create *OSM `xml:"create"`
	Modify *OSM `xml:"modify"`
	Delete *OSM `xml:"delete"`
}

// AppendCreate will append the element to the Create OSM object.
func (c *Change) AppendCreate(e Element) {
	if c.Create == nil {
		c.Create = &OSM{}
	}

	c.Create.Append(e)
}

// AppendModify will append the element to the Modify OSM object.
func (c *Change) AppendModify(e Element) {
	if c.Modify == nil {
		c.Modify = &OSM{}
	}

	c.Modify.Append(e)
}

// AppendDelete will append the element to the Delete OSM object.
func (c *Change) AppendDelete(e Element) {
	if c.Delete == nil {
		c.Delete = &OSM{}
	}

	c.Delete.Append(e)
}

// Marshal encodes the osm change data using protocol buffers.
func (c *Change) Marshal() ([]byte, error) {
	ss := &stringSet{}
	encoded := marshalChange(c, ss, true)
	encoded.Strings = ss.Strings()

	return proto.Marshal(encoded)
}

// UnmarshalChange will unmarshal the data into a Change object.
func UnmarshalChange(data []byte) (*Change, error) {

	pbf := &osmpb.Change{}
	err := proto.Unmarshal(data, pbf)
	if err != nil {
		return nil, err
	}

	return unmarshalChange(pbf, pbf.GetStrings(), nil)
}

func marshalChange(c *Change, ss *stringSet, includeChangeset bool) *osmpb.Change {
	if c == nil {
		return nil
	}

	return &osmpb.Change{
		Create: marshalOSM(c.Create, ss, includeChangeset),
		Modify: marshalOSM(c.Modify, ss, includeChangeset),
		Delete: marshalOSM(c.Delete, ss, includeChangeset),
	}
}

func unmarshalChange(encoded *osmpb.Change, ss []string, cs *Changeset) (*Change, error) {
	var err error
	c := &Change{}

	c.Create, err = unmarshalOSM(encoded.Create, ss, cs)
	if err != nil {
		return nil, err
	}

	c.Modify, err = unmarshalOSM(encoded.Modify, ss, cs)
	if err != nil {
		return nil, err
	}

	c.Delete, err = unmarshalOSM(encoded.Delete, ss, cs)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// MarshalXML implements the xml.Marshaller method to allow for the
// correct wrapper/start element case and attr data.
func (c Change) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	start.Name.Local = "osmChange"
	start.Attr = []xml.Attr{
		{
			Name:  xml.Name{Local: "version"},
			Value: strconv.FormatFloat(c.Version, 'g', -1, 64),
		},
		{Name: xml.Name{Local: "generator"}, Value: c.Generator},
	}

	if err := e.EncodeToken(start); err != nil {
		return err
	}

	if err := marshalInnerChange(e, "create", c.Create); err != nil {
		return err
	}

	if err := marshalInnerChange(e, "modify", c.Modify); err != nil {
		return err
	}

	if err := marshalInnerChange(e, "delete", c.Delete); err != nil {
		return err
	}

	if err := e.EncodeToken(start.End()); err != nil {
		return err
	}

	return nil
}

func marshalInnerChange(e *xml.Encoder, name string, o *OSM) error {
	if o == nil {
		return nil
	}

	t := xml.StartElement{Name: xml.Name{Local: name}}
	if err := e.EncodeToken(t); err != nil {
		return err
	}

	if err := o.marshalInnerXML(e); err != nil {
		return err
	}

	if err := e.EncodeToken(t.End()); err != nil {
		return err
	}

	return nil
}

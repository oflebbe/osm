package osmapi

import (
	"fmt"

	"github.com/paulmach/go.osm"
	"golang.org/x/net/context"
)

// Way returns the latest version of the way from the osm rest api.
// Delegates to the DefaultDataSource and uses its http.Client to make the request.
func Way(ctx context.Context, id osm.WayID) (*osm.Way, error) {
	return DefaultDataSource.Way(ctx, id)
}

// Way returns the latest version of the way from the osm rest api.
func (ds *DataSource) Way(ctx context.Context, id osm.WayID) (*osm.Way, error) {
	url := fmt.Sprintf("%s/way/%d", ds.baseURL(), id)

	o := &osm.OSM{}
	if err := ds.getFromAPI(ctx, url, &o); err != nil {
		return nil, err
	}

	if l := len(o.Ways); l != 1 {
		return nil, fmt.Errorf("wrong number of ways, expected 1, got %v", l)
	}

	return o.Ways[0], nil
}

// WayVersion returns the specific version of the way from the osm rest api.
// Delegates to the DefaultDataSource and uses its http.Client to make the request.
func WayVersion(ctx context.Context, id osm.WayID, v int) (*osm.Way, error) {
	return DefaultDataSource.WayVersion(ctx, id, v)
}

// WayVersion returns the specific version of the way from the osm rest api.
func (ds *DataSource) WayVersion(ctx context.Context, id osm.WayID, v int) (*osm.Way, error) {
	url := fmt.Sprintf("%s/way/%d/%d", ds.baseURL(), id, v)

	o := &osm.OSM{}
	if err := ds.getFromAPI(ctx, url, &o); err != nil {
		return nil, err
	}

	if l := len(o.Ways); l != 1 {
		return nil, fmt.Errorf("wrong number of ways, expected 1, got %v", l)
	}

	return o.Ways[0], nil
}

// WayHistory returns all the versions of the way from the osm rest api.
// Delegates to the DefaultDataSource and uses its http.Client to make the request.
func WayHistory(ctx context.Context, id osm.WayID) (osm.Ways, error) {
	return DefaultDataSource.WayHistory(ctx, id)
}

// WayHistory returns all the versions of the way from the osm rest api.
func (ds *DataSource) WayHistory(ctx context.Context, id osm.WayID) (osm.Ways, error) {
	url := fmt.Sprintf("%s/way/%d/history", ds.baseURL(), id)

	o := &osm.OSM{}
	if err := ds.getFromAPI(ctx, url, &o); err != nil {
		return nil, err
	}

	return o.Ways, nil
}

// WayRelations returns all relations a way is part of.
// There is no error if the element does not exist.
// Delegates to the DefaultDataSource and uses its http.Client to make the request.
func WayRelations(ctx context.Context, id osm.WayID) (osm.Relations, error) {
	return DefaultDataSource.WayRelations(ctx, id)
}

// WayRelations returns all relations a way is part of.
// There is no error if the element does not exist.
func (ds *DataSource) WayRelations(ctx context.Context, id osm.WayID) (osm.Relations, error) {
	url := fmt.Sprintf("%s/way/%d/relations", ds.baseURL(), id)

	o := &osm.OSM{}
	if err := ds.getFromAPI(ctx, url, &o); err != nil {
		return nil, err
	}

	return o.Relations, nil
}

// WayFull returns the way and its nodes for the latest version the way.
// Delegates to the DefaultDataSource and uses its http.Client to make the request.
func WayFull(ctx context.Context, id osm.WayID) (*osm.OSM, error) {
	return DefaultDataSource.WayFull(ctx, id)
}

// WayFull returns the way and its nodes for the latest version the way.
func (ds *DataSource) WayFull(ctx context.Context, id osm.WayID) (*osm.OSM, error) {
	url := fmt.Sprintf("%s/way/%d/full", ds.baseURL(), id)

	o := &osm.OSM{}
	if err := ds.getFromAPI(ctx, url, &o); err != nil {
		return nil, err
	}

	return o, nil
}

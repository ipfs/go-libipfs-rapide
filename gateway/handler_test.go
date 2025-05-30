package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"testing"
	"time"

	"github.com/ipfs/boxo/blocks"
	iface "github.com/ipfs/boxo/coreiface"
	ipath "github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/path/resolver"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/assert"
)

func TestEtagMatch(t *testing.T) {
	for _, test := range []struct {
		header   string // value in If-None-Match HTTP header
		cidEtag  string
		dirEtag  string
		expected bool // expected result of etagMatch(header, cidEtag, dirEtag)
	}{
		{"", `"etag"`, "", false},                        // no If-None-Match
		{"", "", `"etag"`, false},                        // no If-None-Match
		{`"etag"`, `"etag"`, "", true},                   // file etag match
		{`W/"etag"`, `"etag"`, "", true},                 // file etag match
		{`"foo", W/"bar", W/"etag"`, `"etag"`, "", true}, // file etag match (array)
		{`"foo",W/"bar",W/"etag"`, `"etag"`, "", true},   // file etag match (compact array)
		{`"etag"`, "", `W/"etag"`, true},                 // dir etag match
		{`"etag"`, "", `W/"etag"`, true},                 // dir etag match
		{`W/"etag"`, "", `W/"etag"`, true},               // dir etag match
		{`*`, `"etag"`, "", true},                        // wildcard etag match
	} {
		result := etagMatch(test.header, test.cidEtag, test.dirEtag)
		assert.Equalf(t, test.expected, result, "etagMatch(%q, %q, %q)", test.header, test.cidEtag, test.dirEtag)
	}
}

type errorMockAPI struct {
	err error
}

func (api *errorMockAPI) GetUnixFsNode(context.Context, ipath.Resolved) (files.Node, error) {
	return nil, api.err
}

func (api *errorMockAPI) LsUnixFsDir(ctx context.Context, p ipath.Resolved) (<-chan iface.DirEntry, error) {
	return nil, api.err
}

func (api *errorMockAPI) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return nil, api.err
}

func (api *errorMockAPI) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	return nil, api.err
}

func (api *errorMockAPI) GetDNSLinkRecord(ctx context.Context, hostname string) (ipath.Path, error) {
	return nil, api.err
}

func (api *errorMockAPI) IsCached(ctx context.Context, p ipath.Path) bool {
	return false
}

func (api *errorMockAPI) ResolvePath(ctx context.Context, ip ipath.Path) (ipath.Resolved, error) {
	return nil, api.err
}

func TestGatewayBadRequestInvalidPath(t *testing.T) {
	api, _ := newMockAPI(t)
	ts := newTestServer(t, api)
	t.Logf("test server url: %s", ts.URL)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/QmInvalid/Path", nil)
	assert.Nil(t, err)

	res, err := ts.Client().Do(req)
	assert.Nil(t, err)

	assert.Equal(t, http.StatusBadRequest, res.StatusCode)
}

func TestErrorBubblingFromAPI(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{"404 Not Found from IPLD", &ipld.ErrNotFound{}, http.StatusNotFound},
		{"404 Not Found from path resolver", resolver.ErrNoLink{}, http.StatusNotFound},
		{"502 Bad Gateway", ErrBadGateway, http.StatusBadGateway},
		{"504 Gateway Timeout", ErrGatewayTimeout, http.StatusGatewayTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			api := &errorMockAPI{err: fmt.Errorf("wrapped for testing purposes: %w", test.err)}
			ts := newTestServer(t, api)
			t.Logf("test server url: %s", ts.URL)

			req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipns/en.wikipedia-on-ipfs.org", nil)
			assert.Nil(t, err)

			res, err := ts.Client().Do(req)
			assert.Nil(t, err)
			assert.Equal(t, test.status, res.StatusCode)
		})
	}

	for _, test := range []struct {
		name         string
		err          error
		status       int
		headerName   string
		headerValue  string
		headerLength int // how many times was headerName set
	}{
		{"429 Too Many Requests without Retry-After header", ErrTooManyRequests, http.StatusTooManyRequests, "Retry-After", "", 0},
		{"429 Too Many Requests without Retry-After header", NewErrorRetryAfter(ErrTooManyRequests, 0*time.Second), http.StatusTooManyRequests, "Retry-After", "", 0},
		{"429 Too Many Requests with Retry-After header", NewErrorRetryAfter(ErrTooManyRequests, 3600*time.Second), http.StatusTooManyRequests, "Retry-After", "3600", 1},
	} {
		api := &errorMockAPI{err: fmt.Errorf("wrapped for testing purposes: %w", test.err)}
		ts := newTestServer(t, api)
		t.Logf("test server url: %s", ts.URL)

		req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipns/en.wikipedia-on-ipfs.org", nil)
		assert.Nil(t, err)

		res, err := ts.Client().Do(req)
		assert.Nil(t, err)
		assert.Equal(t, test.status, res.StatusCode)
		assert.Equal(t, test.headerValue, res.Header.Get(test.headerName))
		assert.Equal(t, test.headerLength, len(res.Header.Values(test.headerName)))
	}
}

type panicMockAPI struct {
	panicOnHostnameHandler bool
}

func (api *panicMockAPI) GetUnixFsNode(context.Context, ipath.Resolved) (files.Node, error) {
	panic("i am panicking")
}

func (api *panicMockAPI) LsUnixFsDir(ctx context.Context, p ipath.Resolved) (<-chan iface.DirEntry, error) {
	panic("i am panicking")
}

func (api *panicMockAPI) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	panic("i am panicking")
}

func (api *panicMockAPI) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	panic("i am panicking")
}

func (api *panicMockAPI) GetDNSLinkRecord(ctx context.Context, hostname string) (ipath.Path, error) {
	// GetDNSLinkRecord is also called on the WithHostname handler. We have this option
	// to disable panicking here so we can test if both the regular gateway handler
	// and the hostname handler can handle panics.
	if api.panicOnHostnameHandler {
		panic("i am panicking")
	}

	return nil, errors.New("not implemented")
}

func (api *panicMockAPI) IsCached(ctx context.Context, p ipath.Path) bool {
	panic("i am panicking")
}

func (api *panicMockAPI) ResolvePath(ctx context.Context, ip ipath.Path) (ipath.Resolved, error) {
	panic("i am panicking")
}

func TestGatewayStatusCodeOnPanic(t *testing.T) {
	api := &panicMockAPI{}
	ts := newTestServer(t, api)
	t.Logf("test server url: %s", ts.URL)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e", nil)
	assert.Nil(t, err)

	res, err := ts.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
}

func TestGatewayStatusCodeOnHostnamePanic(t *testing.T) {
	api := &panicMockAPI{panicOnHostnameHandler: true}
	ts := newTestServer(t, api)
	t.Logf("test server url: %s", ts.URL)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e", nil)
	assert.Nil(t, err)

	res, err := ts.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
}

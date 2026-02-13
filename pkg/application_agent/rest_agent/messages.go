// SPDX-FileCopyrightText: 2020 Alvar Penning
//
// SPDX-License-Identifier: GPL-3.0-or-later

package rest_agent

import "github.com/dtn7/dtn7-go/pkg/bpv7"

// RestRegisterRequest describes a JSON to be POSTed to /register.
type RestRegisterRequest struct {
	EndpointId string `json:"endpoint_id"`
}

// RestRegisterResponse describes a JSON response for /register.
type RestRegisterResponse struct {
	Error string `json:"error"`
	UUID  string `json:"uuid"`
}

// RestUnregisterRequest describes a JSON to be POSTed to /unregister.
type RestUnregisterRequest struct {
	UUID string `json:"uuid"`
}

// RestUnregisterResponse describes a JSON response for /unregister.
type RestUnregisterResponse struct {
	Error string `json:"error"`
}

// RestFetchRequest describes a JSON to be POSTed to /fetch.
type RestFetchRequest struct {
	UUID   string `json:"uuid"`
	New    bool   `json:"new,omitempty"`    // Optional: fetch only new bundles (default: false)
	Remove bool   `json:"remove,omitempty"` // Optional: remove after fetch (default: true for backward compatibility)
}

// RestFetchResponse describes a JSON response for /fetch.
type RestFetchResponse struct {
	Error   string        `json:"error"`
	Bundles []bpv7.Bundle `json:"bundles"`
}

// RestListRequest describes a JSON to be POSTed to /list.
type RestListRequest struct {
	UUID string `json:"uuid"`
	New  bool   `json:"new,omitempty"` // Optional: list only new bundles (default: false)
}

// RestListResponse describes a JSON response for /list.
type RestListResponse struct {
	Error   string   `json:"error"`
	Bundles []string `json:"bundles"` // Array of bundle IDs
}

// RestFetchBundleRequest describes a JSON to be POSTed to /fetch_bundle.
type RestFetchBundleRequest struct {
	UUID     string `json:"uuid"`
	BundleID string `json:"bundle_id"`
	Remove   bool   `json:"remove,omitempty"` // Optional: remove after fetch (default: false)
}

// RestFetchBundleResponse describes a JSON response for /fetch_bundle.
type RestFetchBundleResponse struct {
	Error  string       `json:"error"`
	Bundle *bpv7.Bundle `json:"bundle"` // Single bundle, null if not found
}

// RestDeleteBundleRequest describes a JSON to be POSTed to /delete_bundle.
type RestDeleteBundleRequest struct {
	UUID     string `json:"uuid"`
	BundleID string `json:"bundle_id"`
}

// RestDeleteBundleResponse describes a JSON response for /delete_bundle.
type RestDeleteBundleResponse struct {
	Error string `json:"error"`
}

// RestBuildRequest describes a JSON to be POSTed to /build.
type RestBuildRequest struct {
	UUID string                 `json:"uuid"`
	Args map[string]interface{} `json:"arguments"`
}

// RestBuildResponse describes a JSON response for /build.
type RestBuildResponse struct {
	Error string `json:"error"`
}

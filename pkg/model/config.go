// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sort"
	"strings"
	"sync"

	udpa "github.com/cncf/udpa/go/udpa/type/v1"

	"istio.io/istio-mcp/pkg/config/schema/resource"
	"istio.io/istio-mcp/pkg/features"
	"istio.io/libistio/pkg/config"
)

const (
	VersionNotInitialized     = ""
	AnnotationResourceVersion = "ResourceVersion"

	IstioRevLabel = "istio.io/rev" // does not refer to that in istio-api to get a better compatibility
)

// Statically link protobuf descriptors from UDPA
var _ = udpa.TypedStruct{}

type NamespacedName struct {
	Name      string
	Namespace string
}

// ConfigKey describe a specific config item.
// In most cases, the name is the config's name. However, for ServiceEntry it is service's FQDN.
type ConfigKey struct {
	Kind      resource.GroupVersionKind
	Name      string
	Namespace string
}

func (key ConfigKey) HashCode() uint32 {
	var result uint32
	result = 31*result + crc32.ChecksumIEEE([]byte(key.Kind.Kind))
	result = 31*result + crc32.ChecksumIEEE([]byte(key.Kind.Version))
	result = 31*result + crc32.ChecksumIEEE([]byte(key.Kind.Group))
	result = 31*result + crc32.ChecksumIEEE([]byte(key.Namespace))
	result = 31*result + crc32.ChecksumIEEE([]byte(key.Name))
	return result
}

func Key(cfg *Config) ConfigKey {
	return ConfigKey{
		Kind:      cfg.GroupVersionKind,
		Name:      cfg.Name,
		Namespace: cfg.Namespace,
	}
}

// ConfigsOfKind extracts configs of the specified kind.
func ConfigsOfKind(configs map[ConfigKey]struct{}, kind resource.GroupVersionKind) map[ConfigKey]struct{} {
	ret := make(map[ConfigKey]struct{})

	for conf := range configs {
		if conf.Kind == kind {
			ret[conf] = struct{}{}
		}
	}

	return ret
}

// ConfigNamesOfKind extracts config names of the specified kind.
func ConfigNamesOfKind(configs map[ConfigKey]struct{}, kind resource.GroupVersionKind) map[string]struct{} {
	ret := make(map[string]struct{})

	for conf := range configs {
		if conf.Kind == kind {
			ret[conf.Name] = struct{}{}
		}
	}

	return ret
}

func SerializeConfigMeta(cm Meta) []byte {
	buf := &bytes.Buffer{}

	sep := func() {
		buf.WriteByte(0)
	}

	addStr := func(strs ...string) {
		for _, s := range strs {
			buf.WriteString(s)
			sep()
		}
	}
	addStrMap := func(m map[string]string) {
		if len(m) > 0 {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				addStr(k, m[k])
			}
		}
		sep()
	}

	addStr(cm.GroupVersionKind.Group, cm.GroupVersionKind.Version, cm.GroupVersionKind.Kind,
		cm.Name, cm.Namespace, cm.Domain, cm.ResourceVersion)

	addStrMap(cm.Labels)
	addStrMap(cm.Annotations)

	intBuf := [8]byte{}
	binary.BigEndian.PutUint64(intBuf[:], uint64(cm.CreationTimestamp.UnixNano()))
	buf.Write(intBuf[:])

	return buf.Bytes()
}

// NOTE:
// we require the ResourceVersion of the Config can simply be compared with `>`, and the newer the version the larger.
type Config = config.Config
type Meta = config.Meta

var ToProto = config.ToProto
var PilotConfigToResource = config.PilotConfigToResource

func CurrentResourceVersion(c *Config) string {
	// c.ResourceVersion: prev
	// resource version in annotation: curr/new
	if annos := c.Annotations; annos == nil {
		return ""
	} else {
		return annos[AnnotationResourceVersion]
	}
}

func ObjectInRevision(c *Config, rev string) bool {
	return RevisionMatch(ConfigIstioRev(c), rev)
}

func ConfigIstioRev(c *Config) string {
	return c.Labels[IstioRevLabel]
}

func RevisionMatch(configRev, rev string) bool {
	if configRev == "" { // This is a global object
		return !features.StrictIstioRev || // global obj always included in non-strict mode
			configRev == rev
	}

	// Otherwise, only return true if ","-joined rev contains config rev
	for rev != "" {
		idx := strings.Index(rev, ",")
		var cur string
		if idx >= 0 {
			cur, rev = rev[:idx], rev[idx+1:]
		} else {
			cur, rev = rev, ""
		}

		if configRev == cur {
			return true
		}
	}
	return false
}

// UpdateConfigToProxyRevision try to update config rev to the first non-empty proxy rev
func UpdateConfigToProxyRevision(c *Config, proxyRev string) bool {
	// find first non-empty rev
	rev := proxyRev
	for idx := strings.Index(proxyRev, ","); idx >= 0; {
		rev = proxyRev[:idx]
		proxyRev = proxyRev[idx+1:]
		if rev != "" {
			break
		}
	}

	return UpdateConfigLabel(c, IstioRevLabel, rev)
}

func UpdateConfigLabel(c *Config, key, value string) bool {
	return updayeConfigAnnoOrLabel(&c.Labels, key, value)
}

func UpdateConfigAnnotation(c *Config, key, value string) bool {
	return updayeConfigAnnoOrLabel(&c.Annotations, key, value)
}

func updayeConfigAnnoOrLabel(mp *map[string]string, k, v string) bool {
	m := *mp
	if m[k] == v { // consider empty value equals not-exist
		return false
	}

	if m == nil {
		m = map[string]string{}
	} else {
		mCopy := make(map[string]string, len(m))
		for mk, mv := range m {
			mCopy[mk] = mv
		}
		m = mCopy
	}

	if v == "" {
		delete(m, k)
	} else {
		m[k] = v
	}
	*mp = m
	return true
}

// UpdateAnnotationResourceVersion check if c.ResourceVersion has changed (different from ver in anno)
// update version in annotation if that
func UpdateAnnotationResourceVersion(c *Config) string {
	annos := c.Annotations
	var prev string
	if annos == nil {
		if c.ResourceVersion == prev {
			return prev
		}
		// new
		annos = map[string]string{}
	} else {
		prev = annos[AnnotationResourceVersion]
		if c.ResourceVersion == prev {
			return prev
		}
		// cow to avoid concurrent-access
		annos = make(map[string]string, len(c.Annotations))
		for k, v := range c.Annotations {
			annos[k] = v
		}
	}

	annos[AnnotationResourceVersion] = c.ResourceVersion
	c.Annotations = annos
	return prev
}

// FilterByGvkAndNamespace returns a subset of configs that match the given gvk and namespace and have newer version than the given version.
func FilterByGvkAndNamespace(configs []Config, gvk resource.GroupVersionKind, namespace, ver string) []Config {
	var (
		allGvk = gvk == resource.AllGvk
		allNs  = namespace == resource.AllNamespace
	)

	if allGvk && allNs {
		return configs
	}

	matcher := func(c Config) bool {
		return (allGvk || c.GroupVersionKind == gvk) && (allNs || c.Namespace == namespace) && (ver == "" || c.ResourceVersion > ver)
	}

	var ret []Config
	for _, c := range configs {
		if matcher(c) {
			ret = append(ret, c)
		}
	}
	return ret
}

type ConfigStore interface {
	Get(gvk resource.GroupVersionKind, namespace, name string) (*Config, error)
	// List returns all configs that match the given gvk and namespace and have newer version than the given version.
	// and return the newest version.
	List(gvk resource.GroupVersionKind, namespace, ver string) ([]Config, string, error)
	// Snapshot if pass all-namespace, will return a snapshot contains all ns's data and use the largest
	// version of them as version.
	Snapshot(ns string) ConfigSnapshot
	// Version if pass all-namespace, will return the largest version of all namespace snapshots.
	Version(ns string) string
	// VersionSnapshot
	// version can not be empty or return nil.
	// Ns can be empty to indicate merging-all-contents-of-same-version-to-one-snapshot.
	VersionSnapshot(version, ns string) ConfigSnapshot
	// VersionByGvk returns the largest version of all configs that match the given gvk.
	VersionByGvk(gvk resource.GroupVersionKind) string
}

// ConfigSnapshot may merge configs of different namespaces but same version into a single snapshot.
type ConfigSnapshot interface {
	Version() string
	VersionByGvk(gvk resource.GroupVersionKind) string
	Config(gvk resource.GroupVersionKind, namespace, name string) *Config
	Configs(gvk resource.GroupVersionKind, namespace, ver string) []Config
	Empty() bool
}

type NsConfigSnapshot struct {
	// snapshots is a map of namespace to snapshot.
	snapshots map[string]ConfigSnapshot
}

func MakeNsConfigSnapshot(snapshots map[string]ConfigSnapshot) NsConfigSnapshot {
	return NsConfigSnapshot{snapshots: snapshots}
}

func (n NsConfigSnapshot) Snapshots() map[string]ConfigSnapshot {
	ret := make(map[string]ConfigSnapshot, len(n.snapshots))
	for k, v := range n.snapshots {
		ret[k] = v
	}
	return ret
}

func (n NsConfigSnapshot) Version() string {
	var ret string
	for _, snapshot := range n.snapshots {
		if snapshot == nil {
			continue
		}
		ver := snapshot.Version()
		if ret == "" || ver > ret {
			ret = ver
		}
	}

	return ret
}

func (n NsConfigSnapshot) VersionByGvk(gvk resource.GroupVersionKind) string {
	var ret string
	for _, snapshot := range n.snapshots {
		if snapshot == nil {
			continue
		}
		ver := snapshot.VersionByGvk(gvk)
		if ret == "" || ver > ret {
			ret = ver
		}
	}
	return ret
}

func (n NsConfigSnapshot) Config(gvk resource.GroupVersionKind, namespace, name string) *Config {
	snap := n.snapshots[namespace]
	if snap == nil {
		return nil
	}
	return snap.Config(gvk, namespace, name)
}

func (n NsConfigSnapshot) Configs(gvk resource.GroupVersionKind, namespace, ver string) []Config {
	if namespace != "" {
		snapshot := n.snapshots[namespace]
		if snapshot == nil {
			return nil
		}
		return snapshot.Configs(gvk, namespace, ver)
	}

	var ret []Config
	for ns, snapshot := range n.snapshots {
		if snapshot == nil {
			continue
		}

		ret = append(ret, snapshot.Configs(gvk, ns, ver)...)
	}

	return ret
}

func (n NsConfigSnapshot) Empty() bool {
	for _, snapshot := range n.snapshots {
		if snapshot == nil {
			continue
		}
		if !snapshot.Empty() {
			return false
		}
	}
	return true
}

type SimpleConfigSnapshot struct {
	version      string
	versionByGvk map[resource.GroupVersionKind]string
	configs      []Config
}

func MakeSimpleConfigSnapshot(configs []Config) SimpleConfigSnapshot {
	ret := SimpleConfigSnapshot{
		configs:      configs,
		versionByGvk: map[resource.GroupVersionKind]string{},
	}
	for _, cfg := range configs {
		ret.versionByGvk[cfg.GroupVersionKind] = cfg.ResourceVersion
		if ret.version == "" || cfg.ResourceVersion > ret.version {
			ret.version = cfg.ResourceVersion
		}
	}
	return ret
}

func (s SimpleConfigSnapshot) Empty() bool {
	return len(s.configs) == 0
}

func (s SimpleConfigSnapshot) Version() string {
	return s.version
}

func (s SimpleConfigSnapshot) VersionByGvk(gvk resource.GroupVersionKind) string {
	return s.versionByGvk[gvk]
}

// Config not that efficient
func (s SimpleConfigSnapshot) Config(gvk resource.GroupVersionKind, namespace, name string) *Config {
	for _, cfg := range s.configs {
		if cfg.Name == name && cfg.Namespace == namespace && cfg.GroupVersionKind == gvk {
			return &cfg
		}
	}
	return nil
}

func (s SimpleConfigSnapshot) Configs(gvk resource.GroupVersionKind, namespace, ver string) []Config {
	return FilterByGvkAndNamespace(s.configs, gvk, namespace, ver)
}

type SimpleConfigStore struct {
	sync.RWMutex
	snaps map[string]ConfigSnapshot
}

func NewSimpleConfigStore() *SimpleConfigStore {
	return &SimpleConfigStore{
		snaps: map[string]ConfigSnapshot{},
	}
}

func (s *SimpleConfigStore) Update(ns string, snap ConfigSnapshot) ConfigSnapshot {
	s.Lock()
	defer s.Unlock()

	prev := s.snaps[ns]
	if snap == nil {
		if prev != nil {
			delete(s.snaps, ns)
		}
	} else {
		s.snaps[ns] = snap
	}
	return prev
}

func (s *SimpleConfigStore) Version(ns string) string {
	s.RLock()
	defer s.RUnlock()

	ret := VersionNotInitialized

	if ns == resource.AllNamespace {
		for _, snap := range s.snaps {
			ver := snap.Version()
			if strings.Compare(ver, ret) > 0 {
				ret = ver
			}
		}
	} else {
		snap := s.snaps[ns]
		if snap != nil {
			ret = snap.Version()
		}
	}

	return ret
}

func (s *SimpleConfigStore) VersionByGvk(gvk resource.GroupVersionKind) string {
	s.RLock()
	defer s.RUnlock()
	ret := VersionNotInitialized
	for _, snap := range s.snaps {
		ver := snap.VersionByGvk(gvk)
		if strings.Compare(ver, ret) > 0 {
			ret = ver
		}
	}
	return ret
}

func (s *SimpleConfigStore) Get(gvk resource.GroupVersionKind, namespace, name string) (*Config, error) {
	s.RLock()
	snap := s.snaps[namespace]
	s.RUnlock()

	if snap == nil {
		return nil, nil
	}

	return snap.Config(gvk, namespace, name), nil
}

func (s *SimpleConfigStore) List(gvk resource.GroupVersionKind, namespace, ver string) ([]Config, string, error) {
	var (
		ret    []Config
		retVer string
	)

	addConfigs := func(cfgs ...Config) {
		for _, cfg := range cfgs {
			if cfg.ResourceVersion > retVer {
				retVer = cfg.ResourceVersion
			}
			ret = append(ret, cfg)
		}
	}

	s.RLock()
	defer s.RUnlock()

	if namespace == resource.AllNamespace {
		for _, snap := range s.snaps {
			addConfigs(snap.Configs(gvk, namespace, ver)...)
		}
	} else {
		snap := s.snaps[namespace]
		if snap != nil {
			addConfigs(snap.Configs(gvk, namespace, ver)...)
		}
	}

	return ret, retVer, nil
}

func (s *SimpleConfigStore) Snapshot(ns string) ConfigSnapshot {
	s.RLock()
	defer s.RUnlock()

	if ns == resource.AllNamespace {
		// copy
		cp := make(map[string]ConfigSnapshot, len(s.snaps))
		for k, v := range s.snaps {
			cp[k] = v
		}
		return MakeNsConfigSnapshot(cp)
	}

	return s.snaps[ns]
}

func (s *SimpleConfigStore) VersionSnapshot(version, ns string) ConfigSnapshot {
	snap := s.Snapshot(ns)
	if snap == nil || snap.Version() != version {
		return nil
	}
	return snap
}

type RecordEmptyConfigStore struct {
	*SimpleConfigStore
	lastN         int
	emptyVersions map[string][]string
	sync.RWMutex
}

func NewRecordEmptyConfigStore(store *SimpleConfigStore, lastN int) *RecordEmptyConfigStore {
	if store == nil {
		store = NewSimpleConfigStore()
	}
	return &RecordEmptyConfigStore{SimpleConfigStore: store, lastN: lastN, emptyVersions: map[string][]string{}}
}

func (s *RecordEmptyConfigStore) Version(ns string) string {
	return s.SimpleConfigStore.Version(ns)
}

func (s *RecordEmptyConfigStore) VersionSnapshot(version, ns string) ConfigSnapshot {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()

	snap := s.SimpleConfigStore.VersionSnapshot(version, ns)
	if snap != nil {
		return snap
	}

	// check for known empty snaps
	for _, r := range s.emptyVersions[ns] {
		if r == version {
			return SimpleConfigSnapshot{
				version: version,
				configs: []Config{},
			}
		}
	}

	return nil
}

func (s *RecordEmptyConfigStore) Update(ns string, snapshot ConfigSnapshot) ConfigSnapshot {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()

	if ns == resource.AllNamespace {
		return nil
	}

	ret := s.SimpleConfigStore.Update(ns, snapshot)

	if s.lastN > 0 {
		var affectedNs [][2]string
		if snapshot == nil || snapshot.Empty() { // only delete or update to empty will cause all-ns snap to be empty.
			allNsSnap := s.SimpleConfigStore.Snapshot(resource.AllNamespace)
			if allNsSnap != nil && allNsSnap.Empty() {
				affectedNs = append(affectedNs, [2]string{resource.AllNamespace, allNsSnap.Version()})
			}
		}
		if snapshot != nil && snapshot.Empty() {
			affectedNs = append(affectedNs, [2]string{ns, snapshot.Version()})
		}

		for _, item := range affectedNs {
			ns, ver := item[0], item[1]
			record := s.emptyVersions[ns]
			if record == nil {
				record = make([]string, s.lastN)
				record[0] = ver
				s.emptyVersions[ns] = record
			} else {
				last := ver
				for i := 0; i < len(record); i++ {
					record[i], last = last, record[i]
				}
			}
		}
	}

	return ret
}

// SPDX-FileCopyrightText: 2020, 2022, 2023 Markus Sommer
// SPDX-FileCopyrightText: 2020, 2021 Alvar Penning
//
// SPDX-License-Identifier: GPL-3.0-or-later

// Package discovery contains code for peer/neighbor discovery of other DTN nodes through UDP multicast packages.
package discovery

import (
	"fmt"
	"time"

	"github.com/schollz/peerdiscovery"
	log "github.com/sirupsen/logrus"

	"github.com/dtn7/dtn7-go/pkg/bpv7"
	"github.com/dtn7/dtn7-go/pkg/cla"
	"github.com/dtn7/dtn7-go/pkg/cla/mtcp"
	"github.com/dtn7/dtn7-go/pkg/cla/quicl"
)

const (
	// address4 is the default multicast IPv4 address used for discovery.
	address4 = "224.23.23.23"

	// address6 is the default multicast IPv4 add6ess used for discovery.
	address6 = "ff02::23"

	// broadcast4 is the default broadcast IPv4 address used for discovery.
	broadcast4 = "255.255.255.255"

	// port is the default multicast UDP port used for discovery.
	port = 35039
)

// Manager publishes and receives Announcements.
type Manager struct {
	NodeId          bpv7.EndpointID
	receiveCallback func(*bpv7.Bundle)

	// UseBroadcast enables broadcast mode instead of multicast
	UseBroadcast bool
	// Override Broadcast address (e.g., "192.168.49.255")
	BroadcastAddr string

	stopChan4 chan struct{}
	stopChan6 chan struct{}

	// Parameters for restart
	announcements        []Announcement
	announcementInterval time.Duration
	ipv4                 bool
	ipv6                 bool
	restartTicker        *time.Ticker
	restartStopChan      chan struct{}
	restartDuration      time.Duration
}

var managerSingleton *Manager

func InitialiseManager(
	nodeId bpv7.EndpointID,
	announcements []Announcement, announcementInterval time.Duration,
	ipv4, ipv6 bool,
	useBroadcast bool, broadcastAddr string,
	restartDurationSeconds int,
	receiveCallback func(*bpv7.Bundle)) error {
	if managerSingleton != nil {
		log.Fatalf("Attempting to access an uninitialised discovery manager. This must never happen!")
	}

	var restartDuration time.Duration
	if restartDurationSeconds > 0 {
		restartDuration = time.Duration(restartDurationSeconds) * time.Second
	}
	// If restartDurationSeconds <= 0, restartDuration remains 0 (never restart)

	var manager = &Manager{
		NodeId:               nodeId,
		receiveCallback:      receiveCallback,
		UseBroadcast:         useBroadcast,
		BroadcastAddr:        broadcastAddr,
		announcements:        announcements,
		announcementInterval: announcementInterval,
		ipv4:                 ipv4,
		ipv6:                 ipv6,
		restartStopChan:      make(chan struct{}),
		restartDuration:      restartDuration,
	}
	if ipv4 {
		manager.stopChan4 = make(chan struct{})
	}
	if ipv6 {
		manager.stopChan6 = make(chan struct{})
	}

	managerSingleton = manager

	// Start discovery
	err := manager.restart()
	if err != nil {
		return err
	}
	// Start periodic restart goroutine only if restartDuration > 0
	if manager.restartDuration > 0 {
		manager.restartTicker = time.NewTicker(manager.restartDuration)
		go manager.periodicRestart()
	}

	return nil
}

// GetManagerSingleton returns the manager singleton-instance.
// Attempting to call this function before manager initialisation will cause the program to panic.
func GetManagerSingleton() *Manager {
	if managerSingleton == nil {
		log.Fatalf("Attempting to access an uninitialised discovery manager. This must never happen!")
	}
	return managerSingleton
}

func (manager *Manager) Shutdown() {
	managerSingleton = nil

	for _, c := range []chan struct{}{manager.stopChan4, manager.stopChan6} {
		if c != nil {
			c <- struct{}{}
		}
	}
}

func (manager *Manager) notify6(discovered peerdiscovery.Discovered) {
	discovered.Address = fmt.Sprintf("[%s]", discovered.Address)

	manager.notify(discovered)
}

func (manager *Manager) notify(discovered peerdiscovery.Discovered) {
	announcements, err := UnmarshalAnnouncements(discovered.Payload)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"discovery": manager,
			"peer":      discovered.Address,
		}).Warn("Peer discovery failed to parse incoming package")

		return
	}

	for _, announcement := range announcements {
		go manager.handleDiscovery(announcement, discovered.Address)
	}
}

func (manager *Manager) handleDiscovery(announcement Announcement, addr string) {
	if manager.NodeId.SameNode(announcement.Endpoint) {
		return
	}

	/*
		log.WithFields(log.Fields{
			"peer":    addr,
			"message": announcement,
		}).Debug("Peer discovery received a message")
	*/

	var conv cla.Convergence
	switch announcement.Type {
	case cla.MTCP:
		conv = mtcp.NewMTCPClient(fmt.Sprintf("%s:%d", addr, announcement.Port), announcement.Endpoint)
	case cla.QUICL:
		conv = quicl.NewDialerEndpoint(fmt.Sprintf("%s:%d", addr, announcement.Port), manager.NodeId, manager.receiveCallback)
	default:
		log.WithField("cType", announcement.Type).Error("Invalid cType")
		return
	}
	cla.GetManagerSingleton().Register(conv)
}

// periodicRestart restarts discovery every minute
func (manager *Manager) periodicRestart() {
	for {
		select {
		case <-manager.restartTicker.C:
			log.Debug("Restarting discovery manager")
			if err := manager.restart(); err != nil {
				log.WithError(err).Warn("Failed to restart discovery manager")
			}
		case <-manager.restartStopChan:
			return
		}
	}
}

// restart stops and restarts the discovery process
func (manager *Manager) restart() error {
	// Stop current discovery
	for _, c := range []chan struct{}{manager.stopChan4, manager.stopChan6} {
		if c != nil {
			select {
			case c <- struct{}{}:
			default:
			}
		}
	}

	// Wait for discovery goroutines to actually terminate before restarting
	time.Sleep(100 * time.Millisecond)

	// Recreate stop channels
	if manager.ipv4 {
		manager.stopChan4 = make(chan struct{})
	}
	if manager.ipv6 {
		manager.stopChan6 = make(chan struct{})
	}

	// Determine the address to use
	discoveryAddr := address4
	if manager.UseBroadcast {
		if manager.BroadcastAddr != "" {
			discoveryAddr = manager.BroadcastAddr
		} else {
			discoveryAddr = broadcast4
		}
	}

	msg, err := MarshalAnnouncements(manager.announcements)
	if err != nil {
		return err
	}

	sets := []struct {
		active           bool
		multicastAddress string
		stopChan         chan struct{}
		ipVersion        peerdiscovery.IPVersion
		notify           func(discovered peerdiscovery.Discovered)
	}{
		{manager.ipv4, discoveryAddr, manager.stopChan4, peerdiscovery.IPv4, manager.notify},
		{manager.ipv6, address6, manager.stopChan6, peerdiscovery.IPv6, manager.notify6},
	}

	for _, set := range sets {
		if !set.active {
			continue
		}

		set := peerdiscovery.Settings{
			Limit:            -1,
			Port:             fmt.Sprintf("%d", port),
			MulticastAddress: set.multicastAddress,
			Payload:          msg,
			Delay:            manager.announcementInterval,
			TimeLimit:        -1,
			StopChan:         set.stopChan,
			AllowSelf:        true,
			IPVersion:        set.ipVersion,
			Notify:           set.notify,
		}

		discoverErrChan := make(chan error)
		go func() {
			_, discoverErr := peerdiscovery.Discover(set)
			discoverErrChan <- discoverErr
		}()

		select {
		case discoverErr := <-discoverErrChan:
			if discoverErr != nil {
				return discoverErr
			}

		case <-time.After(time.Second):
			break
		}
	}

	return nil
}

// Close this Manager.
func (manager *Manager) Close() {
	// Stop periodic restart
	if manager.restartTicker != nil {
		manager.restartTicker.Stop()
	}
	if manager.restartStopChan != nil {
		close(manager.restartStopChan)
	}

	for _, c := range []chan struct{}{manager.stopChan4, manager.stopChan6} {
		if c != nil {
			c <- struct{}{}
		}
	}
}

func (manager *Manager) String() string {
	return fmt.Sprintf("Manager(%v)", manager.NodeId)
}

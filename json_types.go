package main

type ServersData struct {
	Data []ServerData
}

type ServerData struct {
	ID         string `json:"id"`
	Attributes ServerAttributes
}

type ServerAttributes struct {
	State      string
	Parameters ServerParameters
	Statistics ServerStatistics
}

type ServerParameters struct {
	Address string
	Port    int
}

type ServerStatistics struct {
	Connections float64
}

type ServicesData struct {
	Data []ServiceData
}

type ServiceData struct {
	ID         string `json:"id"`
	Attributes ServiceAttributes
}

type ServiceAttributes struct {
	Router     string
	Statistics ServiceStatistics
}

type ServiceStatistics struct {
	Connections      float64
	TotalConnections float64 `json:"total_connections"`
}

type ThreadsData struct {
	Data []ThreadData
}

type ThreadData struct {
	ID         string `json:"id"`
	Attributes ThreadAttributes
}

type ThreadAttributes struct {
	Stats ThreadStats
}

type ThreadStats struct {
	Reads   float64
	Writes  float64
	Errors  float64
	Hangups float64
	Accepts float64

	AvgEventQLen float64 `json:"avg_event_queue_length"`
	MaxEventQLen float64 `json:"max_event_queue_length"`

	CurrDescriptors  float64 `json:"current_descriptors"`
	TotalDescriptors float64 `json:"total_descriptors"`
}

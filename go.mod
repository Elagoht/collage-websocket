// A collage plugin that carries collage-live's pushed fragments over a WebSocket
// instead of an event stream. It is the one piece of the live stack with a
// dependency, which is why it is a plugin of its own.
module github.com/Elagoht/collage-websocket

go 1.26

require (
	github.com/Elagoht/collage v0.24.0
	github.com/Elagoht/collage-live v0.2.1
)

require github.com/coder/websocket v1.8.15

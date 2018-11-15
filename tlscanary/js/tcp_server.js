/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

"use strict";

/**
 * Implementation of a TCP socket-based command server. It is heavily inspired by
 * https://dxr.mozilla.org/mozilla-central/source/netwerk/test/httpserver/httpd.js
 * where you'll likely find the solution of all current issues with this code.
 *
 * This implementation decodes streams as UTF-8 and calls the request handler on
 * every input line.
 */

const {classes: Cc, interfaces: Ci, utils: Cu, results: Cr, Constructor: CC} = Components;

const tcp_server_thread_manager = Cc["@mozilla.org/thread-manager;1"].getService(Ci.nsIThreadManager);
const tcp_server_main_thread = tcp_server_thread_manager.mainThread;

const ServerSocket = CC("@mozilla.org/network/server-socket;1",
                        "nsIServerSocket",
                        "init");
const ConverterInputStream = CC("@mozilla.org/intl/converter-input-stream;1",
                                "nsIConverterInputStream",
                                "init");
const ConverterOutputStream = CC("@mozilla.org/intl/converter-output-stream;1",
                                 "nsIConverterOutputStream",
                                 "init");


ChromeUtils.defineModuleGetter(this, "setTimeout", "resource://gre/modules/Timer.jsm");
ChromeUtils.defineModuleGetter(this, "clearTimeout", "resource://gre/modules/Timer.jsm");


class TCPServer {
    constructor(request_handler, port=-1, loopback_only=true, backlog=-1) {
        this.request_handler = request_handler;
        this.command_port = port;
        this.loopback_only = loopback_only;
        this.backlog = backlog;
        this.socket = null;
        this.listener = new SocketListener(this.request_handler, this);
        this.shutdown_complete = false;
    }
    start() {
        if (this.socket === null) {
            this.shutdown = false;
            this.socket = new ServerSocket(this.command_port, this.loopback_only, this.backlog);
            this.socket.asyncListen(this.listener);
        } else {
            print("WARNING: Ignoring TCPServer.start() on running server");
        }
    }
    stop() {
        if (this.socket !== null) {
            this.shutdown_complete = false;
            this.shutdown = true;
            this.socket.close();
            this.socket = null;
        } else {
            print("WARNING: Ignoring TCPServer.stop() on halted server");
        }
    }
    wait() {
        // Wait for socket listener to shut down
        while (!this.shutdown_complete) {
            tcp_server_main_thread.processNextEvent(true);
        }
    }
    get port() {
        return this.socket !== null ? this.socket.port : null;
    }
    loop() {
        while (this.socket !== null) {
            tcp_server_main_thread.processNextEvent(true);
        }
    }
}


class SocketListener {

    constructor(request_handler, server) {
        this.server = server;
        this.request_handler = request_handler;
    }

    onSocketAccepted(socket, transport) {
        print("DEBUG: Connection from port", transport.host, transport.port);
        try {
            transport.setTimeout(Cr.TIMEOUT_CONNECT, 60);
            transport.setTimeout(Cr.TIMEOUT_READ_WRITE, 60);
            // const input_stream = transport.openInputStream(Cr.OPEN_UNBUFFERED | Cr.OPEN_BLOCKING, 0, 0)
            const input_stream = transport.openInputStream(0, 0, 0)
                .QueryInterface(Ci.nsIAsyncInputStream);
            // const output_stream = transport.openOutputStream(Cr.OPEN_UNBUFFERED | Cr.OPEN_BLOCKING, 0, 0);
            const output_stream = transport.openOutputStream(0, 0, 0);
            const connection = new Connection(socket, transport, input_stream, output_stream);
            const reader = new StreamReader(connection, this.request_handler);
            input_stream.asyncWait(reader, 0, 0, tcp_server_main_thread);
        } catch (error) {
            print("ERROR: Command listener failed handling streams:", error.toString());
            transport.close(Cr.NS_BINDING_ABORTED);
        }
    }

    onStopListening(socket, condition) {
        print("DEBUG: Socket listener stopped accepting connections. Status: 0x" + condition.toString(16));
        this.server.shutdown_complete = true;
    }
}


class Connection {

    constructor(socket, transport, input, output) {
        this.socket = socket;
        this.transport = transport;
        this.input = input;
        this.output = output;
        this.utf_input_stream = new ConverterInputStream(this.input, "UTF-8", 0, 0x0);
        this.utf_output_stream = new ConverterOutputStream(this.output, "UTF-8", 8192, 0x0);
        this.watchdog = null;
        this.set_timeout();
    }

    reply(response) {
        try {
            // Protocol convention is to send one reply per line.
            this.utf_output_stream.writeString(response + "\n");
            this.utf_output_stream.flush();
        } catch (error) {
            print("ERROR: Unable to send reply:", error.toString());
            if (!this.is_alive()) {
                print("DEBUG: Connection has died");
                this.close();
            }
        }
    }

    close() {
        print("DEBUG: Closing connection to", this.transport.host, this.transport.port);
        try {
            this.utf_output_stream.flush();
            this.utf_output_stream.close();
            this.utf_input_stream.close();
            this.output.close();
            this.input.close();
        } catch (error) {
            print("ERROR: Unable to flush and close connection streams", error.toString());
        }
        this.clear_timeout();
        this.transport.close(Cr.NS_OK);
    }

    is_alive() {
        return this.transport.isAlive();
    }

    timeout_handler() {
        print("WARNING: connection timeout", this.transport.host, this.transport.port);
        this.transport.close(Cr.NS_ABORT)
    }

    set_timeout() {
        this.watchdog = setTimeout(this.timeout_handler.bind(this), 30000);
    }

    clear_timeout() {
        if (this.watchdog !== null) {
            clearTimeout(this.watchdog);
            this.watchdog = null;
        }
    }

    reset_timeout() {
        this.clear_timeout();
        this.set_timeout();
    }
}

class StreamReader {

    constructor(connection, request_handler) {
        this.connection = connection;
        this.request_handler = request_handler;
        this.buffer = "";
    }

    onInputStreamReady(input_stream) {

        // First check if there is data available.
        // This check may fail when the peer has closed the connection.
        let data_available = null;
        try {
            data_available = input_stream.available() > 0;
        } catch (error) {
            if (typeof Components === "undefined") {
                // Code is running after main thread context is destroyed. Do nothing.
                return;
            }
            if (error.result === Cr.NS_BASE_STREAM_CLOSED) {
                // This is thrown after every client disconnect (potentially noisy on stdio)
                // and on every .close() on out transport.
                print("DEBUG: Client closed connection from", this.connection.transport.host,
                    this.connection.transport.port);
                this.connection.close();
            } else {
                print("ERROR: Unable to check stream availability:", error.toString());
            }
            if (this.connection.is_alive()) {
                print("WARNING: Connection timeout on", this.connection.transport.host,
                    this.connection.transport.port);
                this.connection.close();
            }
            return;
        }

        // An empty input stream means connection is at EOF, but not closed.
        if (!data_available) {
            print("DEBUG: Stream is at EOL");
            // if (this.buffer.length > 0)
            //     print("WARNING: Dropping non-empty buffer:", this.buffer);
            // this.connection.close();
            return;
        }

        // Interpret available data as UTF-8 strings
        let str = {};
        try {
            // .onInputStreamReady() is called again when not everything was read.
            this.connection.utf_input_stream.readString(8192, str);
        } catch (error) {
            print("ERROR: Unable to read input stream: ", error.toString());
            this.connection.close();
            return;
        }

        // When a read yields empty, the stream was likely closed.
        if (str.value.length === 0) {
            print("DEBUG: Empty read from stream. Assuming stream was closed");
            this.connection.close();
            return;
        }

        // Reset the connection's timer watchdog
        this.connection.reset_timeout();

        this.buffer += str.value;

        // When there is data available, the protocol expects one request per line.
        if (this.buffer[this.buffer.length - 1] === '\n') {
            // The buffer we just read may have included several command lines
            this.buffer.slice(0, -1).split('\n').forEach((function (cmd_str) {
                // print("DEBUG: handling request line:", cmd_str);
                this.request_handler(cmd_str, this.connection);
            }).bind(this));
            // Clear buffer for next incoming lines
            this.buffer = "";
        }

        // Must explicitly chain to receive more callbacks for this connection.
        input_stream.asyncWait(this, 0, 0, tcp_server_main_thread);

    }
}
